#!/usr/bin/env python3

import os
import re
import json
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

POST_FORMAT = 'https://fanbox.cc/@/posts/{post_id}'
POST_REGEXP = [
    re.compile('^https?:\/\/(?P<creator>[^\.]+)\.fanbox\.cc\/posts\/(?P<post_id>\d+)(?:\?.*)?(?:#.*)?$'),
    re.compile('^https?:\/\/(?:www\.)?fanbox\.cc\/@(?P<creator>[^\/]*)\/posts\/(?P<post_id>\d+)(?:\?.*)?(?:#.*)?$')
]
CREATOR_REGEXP = [
    re.compile('^https?:\/\/(?P<creator>[^\.]+)\.fanbox\.cc\/(?:\/.*)?(?:\?.*)?(?:#.*)?$'),
    re.compile('^https?:\/\/(?:www\.)?fanbox\.cc\/@(?P<creator>[^\/]+)(?:\/.*)?(?:\?.*)?(?:#.*)?$')
]

POST_GET_URL = 'https://api.fanbox.cc/post.info?postId={post_id}'
CREATOR_POSTS_URL = 'https://api.fanbox.cc/post.listCreator'
PAGE_LIMIT = 10

class CreatorIterator:
    def __init__(self, fanbox, subscription=None, options=None):
        self.fanbox = fanbox
        self.http = fanbox.http
        self.log = fanbox.log
        self.subscription = subscription
        
        if self.subscription is not None:
            options = hoordu.Dynamic.from_json(self.subscription.options)
            self.state = hoordu.Dynamic.from_json(self.subscription.state)
        else:
            self.state = hoordu.Dynamic()
        
        self.creator = options.creator
        
        self.first_id = None
        self.head_id = self.state.get('head_id')
        self.tail_id = self.state.get('tail_id')
        self.tail_datetime = self.state.get('tail_datetime')
    
    def _save_state(self):
        self.state.head_id = self.head_id
        self.state.tail_id = self.tail_id
        self.state.tail_datetime = self.tail_datetime
        if self.subscription is not None:
            self.subscription.state = self.state.to_json()
    
    def _post_iterator(self, direction=FetchDirection.newer, n=None):
        head = (direction == FetchDirection.newer)
        
        page_size = PAGE_LIMIT if n is None else min(n, PAGE_LIMIT)
        min_id = int(self.head_id) if head and self.head_id is not None else None
        max_id = self.tail_id if not head else None
        max_datetime = self.tail_datetime if not head else None
        
        total = 0
        first_iteration = True
        while True:
            params = {
                'creatorId': self.creator,
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
            
            if first_iteration and (self.head_id is None or direction == FetchDirection.newer):
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
                    self.tail_id = post.id
                    self.tail_datetime = post.publishedDatetime
                
                total += 1
                if n is not None and total >= n:
                    return
            
            if body.nextUrl is None:
                return
            
            first_iteration = False
    
    def fetch(self, direction=FetchDirection.newer, n=None):
        """
        Try to get at least `n` newer or older posts from this search
        depending on the direction.
        Create a RemotePost entry and any associated Files for each post found,
        thumbnails should be downloaded, files are optional.
        Posts should always come ordered in the same way.
        
        Returns a list of the new RemotePost objects.
        """
        
        # TODO store the user id and use it in subsequent updates
        # https://www.pixiv.net/fanbox/creator/{pixiv_id}
        
        if direction == FetchDirection.newer:
            if self.tail_id is None:
                direction = FetchDirection.older
            else:
                n = None
        
        for post in self._post_iterator(direction, n):
            remote_post = self.fanbox._to_remote_post(post, preview=self.subscription is None)
            yield remote_post
            
            if self.subscription is not None:
                self.subscription.feed.append(remote_post)
            
            # always commit changes
            # RemotePost, RemoteTag and the subscription feed are simply a cache
            # the file downloads are more expensive than a call to the database
            self.fanbox.core.commit()
        
        if self.first_id is not None:
            self.head_id = self.first_id
            self.first_id = None
        
        self._save_state()
        if self.subscription is not None:
            self.fanbox.core.add(self.subscription)

class Fanbox:
    name = 'fanbox'
    version = 1
    
    @classmethod
    def config_form(cls):
        return Form('{} config'.format(cls.name),
            ('FANBOXSESSID', Input('FANBOXSESSID cookie', [validators.required]))
        )
    
    @classmethod
    def init(cls, core, parameters=None):
        source = core.source
        
        cls.update(core)
        
        # check if everything is ready to use
        config = hoordu.Dynamic.from_json(source.config)
        
        if not config.defined('FANBOXSESSID'):
            # try to get the values from the parameters
            if parameters is not None:
                config.update(parameters)
                
                source.config = json.dumps(config)
                core.add(source)
        
        if not config.defined('FANBOXSESSID'):
            # but if they're still None, the api can't be used
            return False, cls.config_form()
            
        else:
            # the config contains every required property
            return True, cls(core)
    
    @classmethod
    def update(cls, core):
        source = core.source
        
        if source.version < cls.version:
            # update anything if needed
            
            # if anything was updated, then the db entry should be updated as well
            source.version = cls.version
            core.add(source)
    
    def __init__(self, core, config=None):
        self.core = core
        self.source = core.source
        self.log = core.logger
        self.session = core.session
        
        if config is None:
            config = hoordu.Dynamic.from_json(self.source.config)
        
        self._load_config(config)
        
        self._init_api()
    
    def _load_config(self, config):
        self.FANBOXSESSID = config.FANBOXSESSID
    
    def _init_api(self):
        self.http = requests.Session()
        
        self.http.headers.update({
            'Origin': 'https://www.fanbox.cc',
            'Referer': 'https://www.fanbox.cc/',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:80.0) Gecko/20100101 Firefox/82.0'
        })
        
        cookie = requests.cookies.create_cookie(name='FANBOXSESSID', value=self.FANBOXSESSID)
        self.http.cookies.set_cookie(cookie)
    
    def parse_url(self, url):
        """
        Checks if an url can be downloaded by this plugin.
        
        Returns the remote id if the url corresponds to a single post,
        a Dynamic object that can be passed to search if the url
        corresponds to multiple posts, or None if this plugin can't
        download or create a search with this url.
        """
        
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
        
        return post_id
    
    def _download_file(self, url, filename=None):
        # TODO file downloads should be managed by hoordu
        # so that rate limiting and a download manager can be
        # implemented easily and in a centralized way
        self.log.debug('downloading %s', url)
        
        if filename is not None:
            suffix = '-{}'.format(filename)
            
        else:
            suffix = os.path.splitext(urlparse(url).path)[-1]
            if not suffix.startswith('.'):
                suffix = ''
        
        fd, path = mkstemp(suffix=suffix)
        
        with self.http.get(url, stream=True) as resp:
            resp.raise_for_status()
            resp.raw.read = functools.partial(resp.raw.read, decode_content=True)
            with os.fdopen(fd, 'w+b') as file:
                shutil.copyfileobj(resp.raw, file)
        
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
                
                metadata = {}
                if post.feeRequired != 0:
                    metadata['price'] = post.feeRequired
                
                remote_post = RemotePost(
                    source=self.source,
                    original_id=main_id,
                    url=POST_FORMAT.format(post_id=main_id),
                    title=post.title,
                    type=PostType.collection,
                    post_time=post_time,
                    metadata_=json.dumps(metadata)
                )
                
                if post.isLiked is True:
                    remote_post.favorite = True
                
                # creators are identified by their pixiv id because their name and creatorId can change
                creator_tag = self.core.get_remote_tag(TagCategory.artist, creator_id)
                remote_post.tags.append(creator_tag)
                metadata = hoordu.Dynamic.from_json(creator_tag.metadata_)
                if metadata.get('name', None) != creator_name or metadata.get('slug', None) != creator_slug:
                    metadata.name = creator_name
                    metadata.slug = creator_name
                    creator_tag.metadata_ = metadata.to_json()
                    self.core.add(creator_tag)
                
                for tag in post.tags:
                    remote_tag = self.core.get_remote_tag(TagCategory.general, tag)
                    remote_post.tags.append(remote_tag)
                
                if post.hasAdultContent is True:
                    nsfw_tag = self.core.get_remote_tag(TagCategory.meta, 'nsfw')
                    remote_post.tags.append(nsfw_tag)
                
                self.core.add(remote_post)
        
        if post.type == 'image':
            current_files = {file.metadata_: file for file in remote_post.files}
            
            for image, order in zip(post.body.images, itertools.count(1)):
                id = 'i-{}'.format(image.id)
                file = current_files.get(id)
                
                if file is None:
                    file = File(remote=remote_post, remote_order=order, metadata_=id)
                    self.core.add(file)
                    self.core.flush()
                    self.log.info('found new file for post %s, file order: %s', remote_post.id, order)
                    
                else:
                    file.remote_order = order
                    self.core.add(file)
                
                need_orig = not file.present and not preview
                need_thumb = not file.thumb_present
                
                if need_thumb or need_orig:
                    self.log.info('downloading files for post: %s, file: %r, thumb: %r', remote_post.id, need_orig, need_thumb)
                    
                    orig = self._download_file(image.originalUrl) if need_orig else None
                    thumb = self._download_file(image.thumbnailUrl) if need_thumb else None
                    
                    self.core.import_file(file, orig=orig, thumb=thumb, move=True)
            
            remote_post.comment = post.body.text
            self.core.add(remote_post)
            
        elif post.type == 'file':
            current_files = {file.metadata_: file for file in remote_post.files}
            
            for rfile, order in zip(post.body.files, itertools.count(1)):
                id = 'f-{}'.format(rfile.id)
                file = current_files.get(id)
                
                if file is None:
                    filename = '{0.name}.{0.extension}'.format(rfile)
                    file = File(remote=remote_post, remote_order=order, filename=filename, metadata_=id)
                    self.core.add(file)
                    self.core.flush()
                    self.log.info('found new file for post %s, file order: %s', remote_post.id, order)
                    
                else:
                    file.remote_order = order
                    self.core.add(file)
                
                need_orig = not file.present and not preview
                need_thumb = not file.thumb_present and post.coverImageUrl is not None
                
                if need_thumb or need_orig:
                    self.log.info('downloading files for post: %s, file: %r, thumb: %r', remote_post.id, need_orig, need_thumb)
                    
                    orig = self._download_file(rfile.url) if need_orig else None
                    thumb = self._download_file(post.coverImageUrl) if need_thumb else None
                    
                    self.core.import_file(file, orig=orig, thumb=thumb, move=True)
            
            remote_post.comment = post.body.text
            self.core.add(remote_post)
            
        elif post.type == 'article':
            current_files = {file.metadata_: file for file in remote_post.files}
            
            imagemap = post.body.get('imageMap')
            filemap = post.body.get('fileMap')
            embedmap = post.body.get('embedMap')
            
            order = 1
            
            blog = []
            for block in post.body.blocks:
                if block.type == 'p':
                    blog.append({
                        'type': 'text',
                        'content': block.text + '\n'
                    })
                    
                elif block.type == 'image':
                    id = 'i-{}'.format(block.imageId)
                    file = current_files.get(id)
                    
                    if file is None:
                        file = File(remote=remote_post, remote_order=order, metadata_=id)
                        self.core.add(file)
                        self.core.flush()
                        self.log.info('found new file for post %s, file order: %s', remote_post.id, order)
                        
                    else:
                        file.remote_order = order
                        self.core.add(file)
                    
                    orig_url = imagemap[block.imageId].originalUrl
                    thumb_url = imagemap[block.imageId].thumbnailUrl
                    
                    need_orig = not file.present and not preview
                    need_thumb = not file.thumb_present
                    
                    if need_thumb or need_orig:
                        self.log.info('downloading files for post: %s, order: %r', remote_post.id, file.remote_order)
                        
                        orig = self._download_file(orig_url) if need_orig else None
                        thumb = self._download_file(thumb_url) if need_thumb else None
                        
                        self.core.import_file(file, orig=orig, thumb=thumb, move=True)
                    
                    blog.append({
                        'type': 'file',
                        'order': order
                    })
                    
                    order += 1
                    
                elif block.type == 'file':
                    id = 'f-{}'.format(block.fileId)
                    file = current_files.get(id)
                    
                    if file is None:
                        file = File(remote=remote_post, remote_order=order, metadata_=id)
                        self.core.add(file)
                        self.core.flush()
                        self.log.info('found new file for post %s, file order: %s', remote_post.id, order)
                    
                    orig_url = filemap[block.fileId].url
                    thumb_url = post.coverImageUrl
                    
                    need_orig = not file.present and not preview
                    need_thumb = not file.thumb_present and thumb_url is not None
                    
                    if need_thumb or need_orig:
                        self.log.info('downloading files for post: %s, order: %r', remote_post.id, file.remote_order)
                        
                        orig = self._download_file(orig_url) if need_orig else None
                        thumb = self._download_file(thumb_url) if need_thumb else None
                        
                        self.core.import_file(file, orig=orig, thumb=thumb, move=True)
                    
                    blog.append({
                        'type': 'file',
                        'order': order
                    })
                    
                    order += 1
                    
                elif block.type == 'embed':
                    embed = embedmap[block.embedId]
                    
                    if embed.serviceProvider == 'fanbox':
                        related_post_id = embed.contentId.split('/')[-1]
                        url = POST_FORMAT.format(post_id=related_post_id)
                    
                    else:
                        raise ValueError('unknown embed service provider: {}'.format(embed.serviceProvider))
                    
                    remote_post.related.append(Related(url=url))
                    
                    blog.append({
                        'type': 'text',
                        'content': url
                    })
                    
                else:
                    self.log.warning('unknown blog block: %s', str(block.type))
            
            remote_post.comment = hoordu.Dynamic({'comment': blog}).to_json()
            remote_post.type = PostType.blog
            self.core.add(remote_post)
            
        elif post.type == 'text':
            remote_post.comment = post.body.text
            remote_post.type = PostType.set
            self.core.add(remote_post)
            
        else:
            raise ValueError('unknown post type: {}'.format(post.type))
        
        return remote_post
    
    def download(self, url=None, remote_post=None, preview=False):
        """
        Creates or updates a RemotePost entry along with all the associated Files,
        and downloads all files and thumbnails that aren't present yet.
        
        If remote_post is passed, its original_id will be used and it will be
        updated in place.
        
        If preview is set to True, then only the thumbnails are downloaded.
        
        Returns the downloaded RemotePost object.
        """
        
        if url is None and remote_post is None:
            raise ValueError('either url or remote_post must be passed')
        
        if remote_post is not None:
            post_id = remote_post.original_id.split('_')[0]
            self.log.info('update request for %s', post_id)
            
        else:
            self.log.info('download request for %s', url)
            if url.isdigit():
                post_id = url
                
            else:
                post_id = None
                for regexp in POST_REGEXP:
                    match = regexp.match(url)
                    if match:
                        post_id = match.group('post_id')
                        break
                
                if post_id is None:
                    raise ValueError('unsupported url: {}'.format(repr(url)))
        
        response = self.http.get(POST_GET_URL.format(post_id=post_id))
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
    
    def search(self, options):
        """
        Creates a temporary search for a given set of search options.
        
        Returns a post iterator object.
        """
        
        return CreatorIterator(self, options=options)
    
    def create_subscription(self, name, options=None, iterator=None):
        """
        Creates a Subscription entry for the given search options identified by the given name,
        should not get any posts from the post source.
        """
        
        if iterator is not None:
            options = hoordu.Dynamic({
                'creator': iterator.creator
            })
            state = iterator.state
            
        elif options is not None:
            state = hoordu.Dynamic()
        
        sub = Subscription(
            source=self.source,
            name=name,
            options=options.to_json(),
            state=state.to_json()
        )
        
        self.core.add(sub)
        self.core.flush()
        
        if iterator is not None:
            iterator.subscription = sub
        
        return sub
    
    def get_iterator(self, subscription):
        """
        Gets the post iterator for a specific subscription.
        
        Returns a post iterator object.
        """
        
        return CreatorIterator(self, subscription=subscription)

Plugin = Fanbox


