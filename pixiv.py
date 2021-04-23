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
from xml.sax.saxutils import unescape

import requests
from bs4 import BeautifulSoup

import hoordu
from hoordu.models import *
from hoordu.plugins import *
from hoordu.forms import *


POST_FORMAT = 'https://www.pixiv.net/artworks/{post_id}'
FANBOX_URL_FORMAT = 'https://www.pixiv.net/fanbox/creator/{user_id}'
POST_REGEXP = [
    re.compile('^(?P<post_id>\d+)_p\d+\.[a-zA-Z0-9]+$'),
    re.compile('^https?:\/\/(?:www\.)?pixiv\.net\/([a-zA-Z]{2}\/)?artworks\/(?P<post_id>\d+)(?:\?.*)?(?:#.*)?$', flags=re.IGNORECASE)
]
USER_REGEXP = [
    re.compile('^https?:\/\/(?:www\.)?pixiv\.net\/([a-zA-Z]{2}\/)?users\/(?P<user_id>\d+)(?:\/illustration|\/manga)?(?:\?.*)?(?:#.*)?$', flags=re.IGNORECASE)
]
BOOKMARKS_REGEXP = [
    re.compile('^https?:\/\/(?:www\.)?pixiv\.net\/([a-zA-Z]{2}\/)?users\/(?P<user_id>\d+)\/bookmarks\/artworks(?:\?.*)?(?:#.*)?$', flags=re.IGNORECASE)
]

USER_URL = 'https://www.pixiv.net/en/users/{user_id}'
POST_GET_URL = 'https://www.pixiv.net/ajax/illust/{post_id}'
POST_PAGES_URL = 'https://www.pixiv.net/ajax/illust/{post_id}/pages'
POST_UGOIRA_URL = 'https://www.pixiv.net/ajax/illust/{post_id}/ugoira_meta'
USER_POSTS_URL = 'https://www.pixiv.net/ajax/user/{user_id}/profile/all'
USER_BOOKMARKS_URL = 'https://www.pixiv.net/ajax/user/{user_id}/illusts/bookmarks'
BOOKMARKS_LIMIT = 48


class IllustIterator(IteratorBase):
    def __init__(self, pixiv, subscription=None, options=None):
        super().__init__(pixiv, subscription=subscription, options=options)
        
        self.http = pixiv.http
        self.log = pixiv.log
        
        self.state.head_id = self.state.get('head_id')
        self.state.tail_id = self.state.get('tail_id')
    
    def _iterator(self, direction=FetchDirection.newer, n=None):
        response = self.http.get(USER_POSTS_URL.format(user_id=self.options.user_id))
        response.raise_for_status()
        user_info = hoordu.Dynamic.from_json(response.text)
        if user_info.error is True:
            self.log.error('pixiv api error: %s', user_info.message)
            raise APIError(user_info.message)
        
        body = user_info.body
        
        posts = []
        for bucket in ('illusts', 'manga'):
            # these are [] when empty
            if isinstance(body[bucket], dict):
                posts.extend(body[bucket].keys())
        
        if self.state.tail_id is None:
            direction = FetchDirection.older
            posts = sorted(posts, reverse=True)
            
        elif direction == FetchDirection.newer:
            posts = sorted([id for id in posts if id > self.state.head_id])
            
        else:
            posts = sorted([id for id in posts if id < self.state.tail_id], reverse=True)
        
        if n is not None:
            posts = posts[:n]
        
        for post_id in posts:
            response = self.http.get(POST_GET_URL.format(post_id=post_id))
            response.raise_for_status()
            post = hoordu.Dynamic.from_json(response.text)
            self.log.debug('post json: %s', post)
            
            if post.error is True:
                self.log.error('pixiv api error: %s', post.message)
                raise APIError(post.message)
            
            if self.state.head_id is None:
                self.state.head_id = post_id
            
            remote_post = self.plugin._to_remote_post(post.body, preview=self.subscription is None)
            yield remote_post
            
            if direction == FetchDirection.newer:
                self.state.head_id = post_id
            elif direction == FetchDirection.older:
                self.state.tail_id = post_id
    
    def fetch(self, direction=FetchDirection.newer, n=None):
        for post in self._iterator(direction, n):
            yield post
            
            if self.subscription is not None:
                self.subscription.feed.append(post)
            
            # always commit changes
            # RemotePost, RemoteTag and the subscription feed are simply a cache
            # the file downloads are more expensive than a call to the database
            self.plugin.core.commit()
        
        if self.subscription is not None:
            self.subscription.state = self.state.to_json()
            self.plugin.core.add(self.subscription)

class BookmarkIterator(IteratorBase):
    def __init__(self, pixiv, subscription=None, options=None):
        super().__init__(pixiv, subscription=subscription, options=options)
        
        self.http = pixiv.http
        self.log = pixiv.log
        
        self.first_id = None
        self.state.head_id = self.state.get('head_id')
        self.state.tail_id = self.state.get('tail_id')
        self.state.offset = self.state.get('offset', 0)
    
    def _iterator(self, direction=FetchDirection.newer, n=None):
        head = (direction == FetchDirection.newer)
        head_id = self.state.head_id
        tail_id = self.state.tail_id
        
        head_id = int(head_id) if head and head_id is not None else None
        tail_id = int(tail_id) if not head and tail_id is not None else None
        offset = 0 if head else self.state.offset
        
        total = 0
        first_iteration = True
        while True:
            page_size = BOOKMARKS_LIMIT if n is None else min(n - total, BOOKMARKS_LIMIT)
            params = {
                'tag': '',
                'offset': str(offset),
                'limit': page_size,
                'rest': 'show'
            }
            
            response = self.http.get(USER_BOOKMARKS_URL.format(user_id=self.options.user_id), params=params)
            response.raise_for_status()
            bookmarks_resp = hoordu.Dynamic.from_json(response.text)
            if bookmarks_resp.error is True:
                self.log.error('pixiv api error: %s', user_info.message)
                raise APIError(bookmarks_resp.message)
            
            bookmarks = bookmarks_resp.body.works
            
            if len(bookmarks) == 0:
                return
            
            # this is the offset for the next request, not stored in the state
            offset += len(bookmarks)
            
            if first_iteration and (self.state.head_id is None or direction == FetchDirection.newer):
                self.first_id = bookmarks[0].bookmarkData.id
            
            for bookmark in bookmarks:
                post_id = bookmark.id
                bookmark_id = int(bookmark.bookmarkData.id)
                
                if head_id is not None and bookmark_id <= head_id:
                    return
                
                if tail_id is not None and bookmark_id >= tail_id:
                    # tail_id not None -> direction == FetchDirection.older
                    self.state.offset += 1
                    continue
                
                response = self.http.get(POST_GET_URL.format(post_id=post_id))
                response.raise_for_status()
                post = hoordu.Dynamic.from_json(response.text)
                self.log.debug('post json: %s', post)
                
                if post.error is True:
                    self.log.error('pixiv api error: %s', post.message)
                    raise APIError(post.message)
                
                remote_post = self.plugin._to_remote_post(post.body, preview=self.subscription is None)
                yield remote_post
                
                if direction == FetchDirection.older:
                    self.state.tail_id = bookmark.bookmarkData.id
                    self.state.offset += 1
                
                total +=1
                if n is not None and total >= n:
                    return
            
            first_iteration = False
    
    def fetch(self, direction=FetchDirection.newer, n=None):
        if direction == FetchDirection.newer:
            if self.state.tail_id is None:
                direction = FetchDirection.older
            else:
                n = None
        
        for post in self._iterator(direction, n):
            yield post
            
            if self.subscription is not None:
                self.subscription.feed.append(post)
            
            self.plugin.core.commit()
        
        if self.first_id is not None:
            self.state.head_id = self.first_id
            self.first_id = None
        
        if self.subscription is not None:
            self.subscription.state = self.state.to_json()
            self.plugin.core.add(self.subscription)

class Pixiv(PluginBase):
    name = 'pixiv'
    version = 1
    
    @classmethod
    def config_form(cls):
        return Form('{} config'.format(cls.name),
            ('PHPSESSID', Input('PHPSESSID cookie', [validators.required]))
        )
    
    @classmethod
    def init(cls, core, parameters=None):
        source = core.source
        
        # check if everything is ready to use
        config = hoordu.Dynamic.from_json(source.config)
        
        if not config.defined('PHPSESSID'):
            # try to get the values from the parameters
            if parameters is not None:
                config.update(parameters)
                
                source.config = json.dumps(config)
                core.add(source)
        
        if not config.defined('PHPSESSID'):
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
        super().__init__(core, config)
        
        self._init_api()
    
    def _init_api(self):
        self.http = requests.Session()
        
        self._headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:80.0) Gecko/20100101 Firefox/82.0'
        }
        self.http.headers.update(self._headers)
        
        cookie = requests.cookies.create_cookie(name='PHPSESSID', value=self.config.PHPSESSID)
        self.http.cookies.set_cookie(cookie)
    
    def parse_url(self, url):
        if url.isdigit():
            return url
        
        for regexp in POST_REGEXP:
            match = regexp.match(url)
            if match:
                return match.group('post_id')
        
        for regexp in USER_REGEXP:
            match = regexp.match(url)
            if match:
                return hoordu.Dynamic({
                    'method': 'illusts',
                    'user_id': match.group('user_id')
                })
        
        for regexp in BOOKMARKS_REGEXP:
            match = regexp.match(url)
            if match:
                return hoordu.Dynamic({
                    'method': 'bookmarks',
                    'user_id': match.group('user_id')
                })
        
        return None
    
    def _download_file(self, url):
        cookies = {
            'PHPSESSID': self.config.PHPSESSID
        }
        
        headers = dict(self._headers)
        headers['Referer'] = 'https://www.pixiv.net/'
        
        path, resp = self.core.download(url, headers=headers, cookies=cookies)
        return path
    
    def _to_remote_post(self, post, remote_post=None, preview=False):
        post_id = post.id
        user_id = post.userId
        user_name = post.userName
        user_account = post.userAccount
        # possible timezone issues?
        post_time = dateutil.parser.parse(post.createDate).astimezone(timezone.utc)
        
        if post.illustType == 1:
            post_type = PostType.collection
        else:
            post_type = PostType.set
        
        self.log.info('getting post %s', post_id)
        
        if remote_post is None:
            remote_post = self.session.query(RemotePost).filter(RemotePost.source_id == self.source.id, RemotePost.original_id == post_id).one_or_none()
            
            if remote_post is None:
                self.log.info('creating new post')
                
                try:
                    comment = post.extraData.meta.twitter.description
                except:
                    comment = post.description
                
                remote_post = RemotePost(
                    source=self.source,
                    original_id=post_id,
                    url=POST_FORMAT.format(post_id=post_id),
                    title=post.title,
                    comment=comment,
                    type=post_type,
                    post_time=post_time
                )
                
                if post.likeData:
                    remote_post.favorite = True
                
                user_tag = self.core.get_remote_tag(TagCategory.artist, user_id)
                remote_post.tags.append(user_tag)
                
                if any((user_tag.update_metadata('name', user_name),
                        user_tag.update_metadata('account', user_account))):
                    self.core.add(user_tag)
                
                for tag in post.tags.tags:
                    remote_tag = self.core.get_remote_tag(TagCategory.general, tag.tag)
                    remote_post.tags.append(remote_tag)
                    if tag.defined('romaji'):
                        tag_metadata = hoordu.Dynamic.from_json(remote_tag.metadata_)
                        if tag_metadata.get('romaji', None) != tag.romaji:
                            tag_metadata.romaji = tag.romaji
                            remote_tag.metadata_ = tag_metadata.to_json()
                            self.core.add(remote_tag)
                    
                
                if post.xRestrict >= 1:
                    nsfw_tag = self.core.get_remote_tag(TagCategory.meta, 'nsfw')
                    remote_post.tags.append(nsfw_tag)
                    
                if post.xRestrict >= 2:
                    nsfw_tag = self.core.get_remote_tag(TagCategory.meta, 'extreme')
                    remote_post.tags.append(nsfw_tag)
                
                if post.isOriginal:
                    original_tag = self.core.get_remote_tag(TagCategory.copyright, 'original')
                    remote_post.tags.append(original_tag)
                
                comment_html = BeautifulSoup(post.description, 'html.parser')
                for a in comment_html.select('a'):
                    remote_post.related.append(Related(url=a.text))
                
                self.core.add(remote_post)
        
        if post.illustType == 2:
            # ugoira
            if len(remote_post.files) > 0:
                file = remote_post.files[0]
                
            else:
                file = File(remote=remote_post, remote_order=0)
                self.core.add(file)
                self.core.flush()
                self.log.info('found new file for post %s, file order: %s', remote_post.id, 0)
            
            need_orig = not file.present and not preview
            need_thumb = not file.thumb_present
            
            if need_thumb or need_orig:
                self.log.info('downloading files for post: %s, file: %r, thumb: %r', remote_post.id, need_orig, need_thumb)
                
                orig = None
                if need_orig:
                    response = self.http.get(POST_UGOIRA_URL.format(post_id=post_id))
                    response.raise_for_status()
                    ugoira_meta = hoordu.Dynamic.from_json(response.text).body
                    
                    orig = self._download_file(ugoira_meta.originalSrc)
                    
                    if file.update_metadata('frames', ugoira_meta.frames):
                        self.core.add(file)
                
                thumb = self._download_file(post.urls.small) if need_thumb else None
                
                self.core.import_file(file, orig=orig, thumb=thumb, move=True)
            
        elif post.pageCount == 1:
            # single page illust
            if len(remote_post.files) > 0:
                file = remote_post.files[0]
                
            else:
                file = File(remote=remote_post, remote_order=0)
                self.core.add(file)
                self.core.flush()
                self.log.info('found new file for post %s, file order: %s', remote_post.id, 0)
            
            need_orig = not file.present and not preview
            need_thumb = not file.thumb_present
            
            if need_thumb or need_orig:
                self.log.info('downloading files for post: %s, file: %r, thumb: %r', remote_post.id, need_orig, need_thumb)
                
                orig = self._download_file(post.urls.original) if need_orig else None
                thumb = self._download_file(post.urls.small) if need_thumb else None
                
                self.core.import_file(file, orig=orig, thumb=thumb, move=True)
            
        else:
            # multi page illust or manga
            available = set(range(post.pageCount))
            present = set(file.remote_order for file in remote_post.files)
            
            for order in available - present:
                file = File(remote=remote_post, remote_order=order)
                self.core.add(file)
                self.core.flush()
                self.log.info('found new file for post %s, file order: %s', remote_post.id, order)
            
            response = self.http.get(POST_PAGES_URL.format(post_id=post_id))
            response.raise_for_status()
            pages = hoordu.Dynamic.from_json(response.text).body
            
            for file in remote_post.files:
                need_orig = not file.present and not preview
                need_thumb = not file.thumb_present
                
                if need_thumb or need_orig:
                    self.log.info('downloading files for post: %s, order: %r', remote_post.id, file.remote_order)
                    
                    orig = self._download_file(pages[file.remote_order].urls.original) if need_orig else None
                    thumb = self._download_file(pages[file.remote_order].urls.small) if need_thumb else None
                    
                    self.core.import_file(file, orig=orig, thumb=thumb, move=True)
        
        return remote_post
    
    def download(self, url=None, remote_post=None, preview=False):
        if url is None and remote_post is None:
            raise ValueError('either url or remote_post must be passed')
        
        if remote_post is not None:
            post_id = remote_post.original_id
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
        post = hoordu.Dynamic.from_json(response.text)
        self.log.debug('post json: %s', post)
        
        if post.error is True:
            self.log.error('pixiv api error: %s', post.message)
            return None
        
        return self._to_remote_post(post.body, remote_post=remote_post, preview=preview)
    
    def search_form(self):
        return Form('{} search'.format(self.name),
            ('method', ChoiceInput('method', [
                    ('illusts', 'illustrations'),
                    ('bookmarks', 'bookmarks')
                ], [validators.required()])),
            ('user_id', Input('user id', [validators.required()]))
        )
    
    def get_search_details(self, options):
        response = self.http.get(USER_URL.format(user_id=options.user_id))
        response.raise_for_status()
        html = BeautifulSoup(response.text, 'html.parser')
        
        preload_json = html.select('#meta-preload-data')[0]['content']
        preload = hoordu.Dynamic.from_json(unescape(preload_json))
        
        user = preload.user[str(options.user_id)]
        
        related_urls = []
        if user.webpage:
            related_urls.append(user.webpage)
        
        # it's [] when it's empty
        if isinstance(user.social, dict):
            related_urls.extend(s.url for s in user.social.values())
        
        comment_html = BeautifulSoup(user.commentHtml, 'html.parser')
        related_urls.extend(a.text for a in comment_html.select('a'))
        
        creator_response = self.http.get(FANBOX_URL_FORMAT.format(user_id=options.user_id), allow_redirects=False)
        if creator_response.status_code // 100 == 3:
            related_urls.append(creator_response.headers['Location'])
        
        return SearchDetails(
            hint=user.name,
            title=user.name,
            description=user.comment,
            thumbnail_url=user.imageBig,
            related_urls=related_urls
        )
    
    def search(self, options):
        options = hoordu.Dynamic.from_json(options)
        
        if options.method == 'illusts':
            return IllustIterator(self, options=options)
            
        elif options.method == 'bookmarks':
            return BookmarkIterator(self, options=options)
    
    def get_iterator(self, subscription):
        options = hoordu.Dynamic.from_json(subscription.options)
        
        if options.method == 'illusts':
            return IllustIterator(self, subscription=subscription)
            
        elif options.method == 'bookmarks':
            return BookmarkIterator(self, subscription=subscription)

Plugin = Pixiv


