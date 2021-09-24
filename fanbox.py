#!/usr/bin/env python3

import os
import re
from datetime import datetime, timedelta, timezone
import dateutil.parser
from tempfile import mkstemp
import shutil
from urllib.parse import urlparse, parse_qs
import itertools
import functools

import requests

import hoordu
from hoordu.models import *
from hoordu.plugins import *
from hoordu.forms import *

CREATOR_ID_GET_URL = 'https://www.pixiv.net/fanbox/creator/{pixiv_id}'
CREATOR_GET_URL = 'https://api.fanbox.cc/creator.get?creatorId={creator}'
CREATOR_URL_REGEXP = re.compile('https?:\/\/(?P<creator>[^\.]+)\.fanbox\.cc\/', flags=re.IGNORECASE)
PIXIV_URL = 'https://www.pixiv.net/en/users/{pixiv_id}'

POST_FORMAT = 'https://fanbox.cc/@/posts/{post_id}'
POST_REGEXP = [
    re.compile('^https?:\/\/(?P<creator>[^\.]+)\.fanbox\.cc\/posts\/(?P<post_id>\d+)(?:\?.*)?(?:#.*)?$', flags=re.IGNORECASE),
    re.compile('^https?:\/\/(?:www\.)?fanbox\.cc\/@(?P<creator>[^\/]*)\/posts\/(?P<post_id>\d+)(?:\?.*)?(?:#.*)?$', flags=re.IGNORECASE)
]
CREATOR_REGEXP = [
    re.compile('^https?:\/\/(?:www\.)?fanbox\.cc\/@(?P<creator>[^\/]+)(?:\/.*)?(?:\?.*)?(?:#.*)?$', flags=re.IGNORECASE),
    re.compile('^https?:\/\/(?P<creator>[^\.]+)\.fanbox\.cc(?:\/.*)?(?:\?.*)?(?:#.*)?$', flags=re.IGNORECASE),
]

POST_GET_URL = 'https://api.fanbox.cc/post.info?postId={post_id}'
CREATOR_POSTS_URL = 'https://api.fanbox.cc/post.listCreator'
PAGE_LIMIT = 10

class CreatorIterator(IteratorBase):
    def __init__(self, fanbox, subscription=None, options=None):
        super().__init__(fanbox, subscription=subscription, options=options)
        
        self.http = fanbox.http
        
        self.options.pixiv_id = self.options.get('pixiv_id')
        
        self.first_id = None
        self.state.head_id = self.state.get('head_id')
        self.state.tail_id = self.state.get('tail_id')
        self.state.tail_datetime = self.state.get('tail_datetime')
    
    def init(self):
        update = False
        
        if self.options.pixiv_id is not None:
            creator = self.plugin._get_creator_id(self.options.pixiv_id)
            
            if creator and self.options.creator != creator:
                self.options.creator = self.options.creator = creator
                update = True
            
        else:
            response = self.http.get(CREATOR_GET_URL.format(creator=self.options.creator))
            response.raise_for_status()
            creator = hoordu.Dynamic.from_json(response.text).body
            
            self.options.pixiv_id = creator.user.userId
            update = True
        
        if update and self.subscription is not None:
            self.subscription.repr = self.plugin.subscription_repr(self.options)
            self.subscription.options = self.options.to_json()
            self.session.add(self.subscription)
    
    def _post_iterator(self, direction=FetchDirection.newer, n=None):
        head = (direction == FetchDirection.newer)
        
        min_id = int(self.state.head_id) if head and self.state.head_id is not None else None
        max_id = self.state.tail_id if not head else None
        max_datetime = self.state.tail_datetime if not head else None
        
        total = 0
        first_iteration = True
        while True:
            page_size = PAGE_LIMIT if n is None else min(n - total, PAGE_LIMIT)
            
            params = {
                'creatorId': self.options.creator,
                'limit': page_size
            }
            
            if max_id is not None:
                params['maxId'] = int(max_id) - 1
                # very big assumption that no posts have the time timestamp
                # fanbox would break if that happened as well
                d = dateutil.parser.parse(max_datetime).replace(tzinfo=None)
                params['maxPublishedDatetime'] = (d - timedelta(seconds=1)).strftime('%Y-%m-%d %H:%M:%S')
            
            response = self.http.get(CREATOR_POSTS_URL, params=params)
            response.raise_for_status()
            body = hoordu.Dynamic.from_json(response.text).body
            posts = body['items']
            
            if len(posts) == 0:
                return
            
            if first_iteration and (self.state.head_id is None or direction == FetchDirection.newer):
                self.first_id = posts[0].id
            
            for post in posts:
                id = int(post.id)
                if min_id is not None and id <= min_id:
                    return
                
                # posts the user has no access to have no body
                if post.body is not None:
                    yield post
                
                max_id = id - 1
                max_datetime = post.publishedDatetime
                
                if direction == FetchDirection.older:
                    self.state.tail_id = post.id
                    self.state.tail_datetime = post.publishedDatetime
                
                total += 1
                if n is not None and total >= n:
                    return
            
            if body.nextUrl is None:
                return
            
            first_iteration = False
    
    def fetch(self, direction=FetchDirection.newer, n=None):
        if direction == FetchDirection.newer:
            if self.state.tail_id is None:
                direction = FetchDirection.older
            else:
                n = None
        
        for post in self._post_iterator(direction, n):
            remote_post = self.plugin._to_remote_post(post, preview=self.subscription is None)
            yield remote_post
            
            if self.subscription is not None:
                self.subscription.feed.append(remote_post)
            
            self.session.commit()
        
        if self.first_id is not None:
            self.state.head_id = self.first_id
            self.first_id = None
        
        if self.subscription is not None:
            self.subscription.state = self.state.to_json()
            self.session.add(self.subscription)
        
        self.session.commit()

class Fanbox(PluginBase):
    name = 'fanbox'
    version = 1
    iterator = CreatorIterator
    
    @classmethod
    def config_form(cls):
        return Form('{} config'.format(cls.name),
            ('FANBOXSESSID', Input('FANBOXSESSID cookie', [validators.required]))
        )
    
    @classmethod
    def setup(cls, session, parameters=None):
        source = cls.get_source(session)
        
        # check if everything is ready to use
        config = hoordu.Dynamic.from_json(source.config)
        
        if not config.defined('FANBOXSESSID'):
            # try to get the values from the parameters
            if parameters is not None:
                config.update(parameters)
                
                source.config = config.to_json()
                session.add(source)
        
        if not config.defined('FANBOXSESSID'):
            # but if they're still None, the api can't be used
            return False, cls.config_form()
            
        else:
            # the config contains every required property
            return True, None
    
    @classmethod
    def update(cls, session):
        source = cls.get_source(session)
        
        if source.version < cls.version:
            # update anything if needed
            
            # if anything was updated, then the db entry should be updated as well
            source.version = cls.version
            session.add(source)
    
    @classmethod
    def parse_url(cls, url):
        if url.isdigit():
            return url
        
        for regexp in POST_REGEXP:
            match = regexp.match(url)
            if match:
                return match.group('post_id')
        
        for regexp in CREATOR_REGEXP:
            match = regexp.match(url)
            if match:
                return hoordu.Dynamic({
                    'creator': match.group('creator')
                })
        
        return None
    
    def __init__(self, session):
        super().__init__(session)
        
        self.http = requests.Session()
        
        self._headers = {
            'Origin': 'https://www.fanbox.cc',
            'Referer': 'https://www.fanbox.cc/',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:80.0) Gecko/20100101 Firefox/82.0'
        }
        self.http.headers.update(self._headers)
        
        cookie = requests.cookies.create_cookie(name='FANBOXSESSID', value=self.config.FANBOXSESSID)
        self.http.cookies.set_cookie(cookie)
    
    def _get_creator_id(self, pixiv_id):
        response = self.http.get(CREATOR_ID_GET_URL.format(pixiv_id=pixiv_id), allow_redirects=False)
        creator_url = response.headers['Location']
        
        match = CREATOR_URL_REGEXP.match(creator_url)
        return match.group('creator')
    
    def _download_file(self, url):
        cookies = {
            'FANBOXSESSID': self.config.FANBOXSESSID
        }
        path, resp = self.session.download(url, headers=self._headers, cookies=cookies)
        return path
    
    def _to_remote_post(self, post, remote_post=None, preview=False):
        main_id = post.id
        creator_id = post.user.userId
        creator_slug = post.creatorId
        creator_name = post.user.name
        # possible timezone issues?
        post_time = dateutil.parser.parse(post.publishedDatetime).astimezone(timezone.utc)
        
        self.log.info('getting post %s', main_id)
        
        if remote_post is None:
            remote_post = self.session.query(RemotePost).filter(RemotePost.source_id == self.source.id, RemotePost.original_id == main_id).one_or_none()
            
            if remote_post is None:
                self.log.info('creating new post')
                
                metadata = hoordu.Dynamic()
                if post.feeRequired != 0:
                    metadata.price = post.feeRequired
                
                remote_post = RemotePost(
                    source=self.source,
                    original_id=main_id,
                    url=POST_FORMAT.format(post_id=main_id),
                    title=post.title,
                    type=PostType.collection,
                    post_time=post_time,
                    metadata_=metadata.to_json()
                )
                
                if post.isLiked is True:
                    remote_post.favorite = True
                
                # creators are identified by their pixiv id because their name and creatorId can change
                creator_tag = self._get_tag(TagCategory.artist, creator_id)
                remote_post.tags.append(creator_tag)
                
                if any((creator_tag.update_metadata('name', creator_name),
                        creator_tag.update_metadata('slug', creator_slug))):
                    self.session.add(creator_tag)
                
                for tag in post.tags:
                    remote_tag = self._get_tag(TagCategory.general, tag)
                    remote_post.tags.append(remote_tag)
                
                if post.hasAdultContent is True:
                    nsfw_tag = self._get_tag(TagCategory.meta, 'nsfw')
                    remote_post.tags.append(nsfw_tag)
                
                self.session.add(remote_post)
        
        current_files = {file.metadata_: file for file in remote_post.files}
        current_urls = [r.url for r in remote_post.related]
        
        if post.type == 'image':
            
            for image, order in zip(post.body.images, itertools.count(1)):
                id = 'i-{}'.format(image.id)
                file = current_files.get(id)
                
                if file is None:
                    file = File(remote=remote_post, remote_order=order, metadata_=id)
                    self.session.add(file)
                    self.session.flush()
                    self.log.info('found new file for post %s, file order: %s', remote_post.id, order)
                    
                else:
                    file.remote_order = order
                    self.session.add(file)
                
                need_orig = not file.present and not preview
                need_thumb = not file.thumb_present
                
                if need_thumb or need_orig:
                    self.log.info('downloading files for post: %s, file: %r, thumb: %r', remote_post.id, need_orig, need_thumb)
                    
                    orig = self._download_file(image.originalUrl) if need_orig else None
                    thumb = self._download_file(image.thumbnailUrl) if need_thumb else None
                    
                    self.session.import_file(file, orig=orig, thumb=thumb, move=True)
            
            remote_post.comment = post.body.text
            self.session.add(remote_post)
            
        elif post.type == 'file':
            for rfile, order in zip(post.body.files, itertools.count(1)):
                id = 'f-{}'.format(rfile.id)
                file = current_files.get(id)
                
                if file is None:
                    filename = '{0.name}.{0.extension}'.format(rfile)
                    file = File(remote=remote_post, remote_order=order, filename=filename, metadata_=id)
                    self.session.add(file)
                    self.session.flush()
                    self.log.info('found new file for post %s, file order: %s', remote_post.id, order)
                    
                else:
                    file.remote_order = order
                    self.session.add(file)
                
                need_orig = not file.present and not preview
                need_thumb = not file.thumb_present and post.coverImageUrl is not None
                
                if need_thumb or need_orig:
                    self.log.info('downloading files for post: %s, file: %r, thumb: %r', remote_post.id, need_orig, need_thumb)
                    
                    orig = self._download_file(rfile.url) if need_orig else None
                    thumb = self._download_file(post.coverImageUrl) if need_thumb else None
                    
                    self.session.import_file(file, orig=orig, thumb=thumb, move=True)
            
            remote_post.comment = post.body.text
            self.session.add(remote_post)
            
        elif post.type == 'article':
            imagemap = post.body.get('imageMap')
            filemap = post.body.get('fileMap')
            embedmap = post.body.get('embedMap')
            
            order = 1
            
            blog = []
            for block in post.body.blocks:
                if block.type in ('p', 'header'):
                    links = block.get('links')
                    if links is not None:
                        for link in links:
                            url = link.url
                            if url not in current_urls:
                                remote_post.related.append(Related(url=url))
                    
                    blog.append({
                        'type': 'text',
                        'content': block.text + '\n'
                    })
                    
                elif block.type == 'image':
                    id = 'i-{}'.format(block.imageId)
                    file = current_files.get(id)
                    
                    if file is None:
                        file = File(remote=remote_post, remote_order=order, metadata_=id)
                        self.session.add(file)
                        self.session.flush()
                        self.log.info('found new file for post %s, file order: %s', remote_post.id, order)
                        
                    else:
                        file.remote_order = order
                        self.session.add(file)
                    
                    orig_url = imagemap[block.imageId].originalUrl
                    thumb_url = imagemap[block.imageId].thumbnailUrl
                    
                    need_orig = not file.present and not preview
                    need_thumb = not file.thumb_present
                    
                    if need_thumb or need_orig:
                        self.log.info('downloading files for post: %s, order: %r', remote_post.id, file.remote_order)
                        
                        orig = self._download_file(orig_url) if need_orig else None
                        thumb = self._download_file(thumb_url) if need_thumb else None
                        
                        self.session.import_file(file, orig=orig, thumb=thumb, move=True)
                    
                    blog.append({
                        'type': 'file',
                        'metadata': id
                    })
                    
                    order += 1
                    
                elif block.type == 'file':
                    id = 'f-{}'.format(block.fileId)
                    file = current_files.get(id)
                    
                    if file is None:
                        file = File(remote=remote_post, remote_order=order, metadata_=id)
                        self.session.add(file)
                        self.session.flush()
                        self.log.info('found new file for post %s, file order: %s', remote_post.id, order)
                    
                    orig_url = filemap[block.fileId].url
                    thumb_url = post.coverImageUrl
                    
                    need_orig = not file.present and not preview
                    need_thumb = not file.thumb_present and thumb_url is not None
                    
                    if need_thumb or need_orig:
                        self.log.info('downloading files for post: %s, order: %r', remote_post.id, file.remote_order)
                        
                        orig = self._download_file(orig_url) if need_orig else None
                        thumb = self._download_file(thumb_url) if need_thumb else None
                        
                        self.session.import_file(file, orig=orig, thumb=thumb, move=True)
                    
                    blog.append({
                        'type': 'file',
                        'metadata': id
                    })
                    
                    order += 1
                    
                elif block.type == 'embed':
                    embed = embedmap[block.embedId]
                    
                    if embed.serviceProvider == 'fanbox':
                        related_post_id = embed.contentId.split('/')[-1]
                        url = POST_FORMAT.format(post_id=related_post_id)
                        
                    elif embed.serviceProvider == 'google_forms':
                        url = 'https://docs.google.com/forms/d/e/{}/viewform'.format(embed.contentId)
                        
                    elif embed.serviceProvider == 'twitter':
                        url = 'https://twitter.com/i/web/status/{}'.format(embed.contentId)
                        
                    else:
                        raise NotImplementedError('unknown embed service provider: {}'.format(embed.serviceProvider))
                    
                    if url not in current_urls:
                        remote_post.related.append(Related(url=url))
                    
                    blog.append({
                        'type': 'text',
                        'content': url + '\n'
                    })
                    
                else:
                    self.log.warning('unknown blog block: %s', str(block.type))
            
            remote_post.comment = hoordu.Dynamic({'comment': blog}).to_json()
            remote_post.type = PostType.blog
            self.session.add(remote_post)
            
        elif post.type == 'text':
            remote_post.comment = post.body.text
            remote_post.type = PostType.set
            self.session.add(remote_post)
            
        else:
            raise NotImplementedError('unknown post type: {}'.format(post.type))
        
        return remote_post
    
    def download(self, id=None, remote_post=None, preview=False):
        if id is None and remote_post is None:
            raise ValueError('either id or remote_post must be passed')
        
        if remote_post is not None:
            id = remote_post.original_id
            self.log.info('update request for %s', id)
        
        response = self.http.get(POST_GET_URL.format(post_id=id))
        response.raise_for_status()
        post = hoordu.Dynamic.from_json(response.text).body
        self.log.debug('post json: %s', post)
        
        if post.body is None:
            self.log.warning('inaccessible post %s', main_id)
            return None
        
        return self._to_remote_post(post, remote_post=remote_post, preview=preview)
    
    def search_form(self):
        return Form('{} search'.format(self.name),
            ('creator', Input('creator', [validators.required()]))
        )
    
    def get_search_details(self, options):
        pixiv_id = options.get('pixiv_id')
        
        creator_id = self._get_creator_id(pixiv_id) if pixiv_id else options.creator 
        
        response = self.http.get(CREATOR_GET_URL.format(creator=creator_id))
        response.raise_for_status()
        creator = hoordu.Dynamic.from_json(response.text).body
        
        options.creator = creator_id
        options.pixiv_id = creator.user.userId
        
        related_urls = creator.profileLinks
        related_urls.append(PIXIV_URL.format(pixiv_id=pixiv_id))
        
        return SearchDetails(
            hint=creator.creatorId,
            title=creator.user.name,
            description=creator.description,
            thumbnail_url=creator.user.iconUrl,
            related_urls=creator.profileLinks
        )
    
    def subscription_repr(self, options):
        return 'posts:{}'.format(options.pixiv_id)

Plugin = Fanbox


