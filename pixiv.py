#!/usr/bin/env python3

import os
import re
from datetime import datetime, timedelta, timezone
import dateutil.parser
from tempfile import mkstemp
import shutil
from urllib.parse import unquote
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
REDIRECT_REGEXP = re.compile('^https?:\/\/(?:www\.)?pixiv\.net\/jump\.php\?(?P<url>.*)$', flags=re.IGNORECASE)

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
        
        self.state.head_id = self.state.get('head_id')
        self.state.tail_id = self.state.get('tail_id')
    
    def _iterator(self):
        response = self.http.get(USER_POSTS_URL.format(user_id=self.options.user_id))
        response.raise_for_status()
        user_info = hoordu.Dynamic.from_json(response.text)
        if user_info.error is True:
            raise APIError(user_info.message)
        
        body = user_info.body
        
        posts = []
        for bucket in ('illusts', 'manga'):
            # these are [] when empty
            if isinstance(body[bucket], dict):
                posts.extend(body[bucket].keys())
        
        if self.state.tail_id is None:
            self.direction = FetchDirection.older
            posts = sorted(posts, reverse=True)
            
        elif self.direction == FetchDirection.newer:
            posts = sorted([id for id in posts if id > self.state.head_id])
            
        else:
            posts = sorted([id for id in posts if id < self.state.tail_id], reverse=True)
        
        if self.num_posts is not None:
            posts = posts[:self.num_posts]
        
        for post_id in posts:
            response = self.http.get(POST_GET_URL.format(post_id=post_id))
            response.raise_for_status()
            post = hoordu.Dynamic.from_json(response.text)
            
            if post.error is True:
                raise APIError(post.message)
            
            if self.state.head_id is None:
                self.state.head_id = post_id
            
            remote_post = self.plugin._to_remote_post(post.body, preview=self.subscription is None)
            yield remote_post
            
            if self.direction == FetchDirection.newer:
                self.state.head_id = post_id
            elif self.direction == FetchDirection.older:
                self.state.tail_id = post_id
    
    def _generator(self):
        for post in self._iterator():
            yield post
            
            if self.subscription is not None:
                self.subscription.feed.append(post)
            
            self.session.commit()
        
        if self.subscription is not None:
            self.subscription.state = self.state.to_json()
            self.session.add(self.subscription)
        
        self.session.commit()

class BookmarkIterator(IteratorBase):
    def __init__(self, pixiv, subscription=None, options=None):
        super().__init__(pixiv, subscription=subscription, options=options)
        
        self.http = pixiv.http
        
        self.first_id = None
        self.state.head_id = self.state.get('head_id')
        self.state.tail_id = self.state.get('tail_id')
        self.state.offset = self.state.get('offset', 0)
    
    def reconfigure(self, direction=FetchDirection.newer, num_posts=None):
        if direction == FetchDirection.newer:
            if self.state.tail_id is None:
                direction = FetchDirection.older
            else:
                num_posts = None
        
        super().reconfigure(direction=direction, num_posts=num_posts)
    
    def _iterator(self):
        head = (self.direction == FetchDirection.newer)
        head_id = self.state.head_id
        tail_id = self.state.tail_id
        
        head_id = int(head_id) if head and head_id is not None else None
        tail_id = int(tail_id) if not head and tail_id is not None else None
        offset = 0 if head else self.state.offset
        
        total = 0
        first_iteration = True
        while True:
            if total > 0:
                page_size = BOOKMARKS_LIMIT if self.num_posts is None else min(self.num_posts - total, BOOKMARKS_LIMIT)
                
            else:
                # request full pages until it finds the first new id
                page_size = BOOKMARKS_LIMIT
            
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
                raise APIError(bookmarks_resp.message)
            
            bookmarks = bookmarks_resp.body.works
            
            if len(bookmarks) == 0:
                return
            
            # this is the offset for the next request, not stored in the state
            offset += len(bookmarks)
            
            if first_iteration and (self.state.head_id is None or self.direction == FetchDirection.newer):
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
                
                if post.error is True:
                    raise APIError(post.message)
                
                remote_post = self.plugin._to_remote_post(post.body, preview=self.subscription is None)
                yield remote_post
                
                if self.direction == FetchDirection.older:
                    self.state.tail_id = bookmark.bookmarkData.id
                    self.state.offset += 1
                
                total +=1
                if self.num_posts is not None and total >= self.num_posts:
                    return
            
            first_iteration = False
    
    def _generator(self):
        for post in self._iterator():
            yield post
            
            if self.subscription is not None:
                self.subscription.feed.append(post)
            
            self.session.commit()
        
        if self.first_id is not None:
            self.state.head_id = self.first_id
            self.first_id = None
        
        if self.subscription is not None:
            self.subscription.state = self.state.to_json()
            self.session.add(self.subscription)
        
        self.session.commit()

class Pixiv(SimplePluginBase):
    name = 'pixiv'
    version = 1
    
    @classmethod
    def config_form(cls):
        return Form('{} config'.format(cls.name),
            ('PHPSESSID', Input('PHPSESSID cookie', [validators.required]))
        )
    
    @classmethod
    def setup(cls, session, parameters=None):
        source = cls.get_source(session)
        
        # check if everything is ready to use
        config = hoordu.Dynamic.from_json(source.config)
        
        if not config.defined('PHPSESSID'):
            # try to get the values from the parameters
            if parameters is not None:
                config.update(parameters)
                
                source.config = config.to_json()
                session.add(source)
        
        if not config.defined('PHPSESSID'):
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
    
    def __init__(self, session):
        super().__init__(session)
        
        self.http = requests.Session()
        
        self._headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:80.0) Gecko/20100101 Firefox/82.0'
        }
        self.http.headers.update(self._headers)
        
        cookie = requests.cookies.create_cookie(name='PHPSESSID', value=self.config.PHPSESSID)
        self.http.cookies.set_cookie(cookie)
    
    def _download_file(self, url):
        cookies = {
            'PHPSESSID': self.config.PHPSESSID
        }
        
        headers = dict(self._headers)
        headers['Referer'] = 'https://www.pixiv.net/'
        
        path, resp = self.session.download(url, headers=headers, cookies=cookies)
        return path
    
    def _parse_href(self, page_url, href):
        if re.match('^https?:\/\/\S+$', href):
            return href
        
        if href.startswith('/'):
            base_url = re.match('^[^:]+:\/\/[^\/]+', page_url).group(0)
            return base_url + href
        
        else:
            base_url = re.match('^.*/', page_url).group(0)
            return base_url + href
        
    
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
        
        if remote_post is None:
            remote_post = self._get_post(post_id)
        
        if remote_post is None:
            remote_post = RemotePost(
                source=self.source,
                original_id=post_id,
                url=POST_FORMAT.format(post_id=post_id),
                title=post.title,
                type=post_type,
                post_time=post_time
            )
            
            self.session.add(remote_post)
            self.session.flush()
        
        self.log.info(f'downloading post: {remote_post.original_id}')
        self.log.info(f'local id: {remote_post.id}')
        
        # there is no visual difference in multiple whitespace (or newlines for that matter)
        # unless inside <pre>, but that's too hard to deal with :(
        description = re.sub('\s+', ' ', post.description)
        comment_html = BeautifulSoup(description, 'html.parser')
        
        urls = []
        page_url = POST_FORMAT.format(post_id=post_id)
        for a in comment_html.select('a'):
            url = self._parse_href(page_url, a['href'])
            match = REDIRECT_REGEXP.match(url)
            if match:
                url = unquote(match.group('url'))
                urls.append(url)
                
            else:
                urls.append(url)
            
            a.replace_with(url)
        
        for br in comment_html.find_all('br'):
            br.replace_with('\n')
        
        remote_post.comment = comment_html.text
        
        if post.likeData:
            remote_post.favorite = True
        
        user_tag = self._get_tag(TagCategory.artist, user_id)
        remote_post.add_tag(user_tag)
        
        if any((user_tag.update_metadata('name', user_name),
                user_tag.update_metadata('account', user_account))):
            self.session.add(user_tag)
        
        for tag in post.tags.tags:
            remote_tag = self._get_tag(TagCategory.general, tag.tag)
            remote_post.add_tag(remote_tag)
            
            if tag.defined('romaji') and remote_tag.update_metadata('romaji', tag.romaji):
                self.session.add(remote_tag)
        
        if post.xRestrict >= 1:
            nsfw_tag = self._get_tag(TagCategory.meta, 'nsfw')
            remote_post.add_tag(nsfw_tag)
            
        if post.xRestrict >= 2:
            nsfw_tag = self._get_tag(TagCategory.meta, 'extreme')
            remote_post.add_tag(nsfw_tag)
        
        if post.isOriginal:
            original_tag = self._get_tag(TagCategory.copyright, 'original')
            remote_post.add_tag(original_tag)
        
        for url in urls:
            remote_post.add_related_url(url)
        
        # files
        if post.illustType == 2:
            # ugoira
            if len(remote_post.files) > 0:
                file = remote_post.files[0]
                
            else:
                file = File(remote=remote_post, remote_order=0)
                self.session.add(file)
                self.session.flush()
            
            need_orig = not file.present and not preview
            need_thumb = not file.thumb_present
            
            if need_thumb or need_orig:
                self.log.info(f'downloading file: {file.remote_order}')
                
                orig = None
                if need_orig:
                    response = self.http.get(POST_UGOIRA_URL.format(post_id=post_id))
                    response.raise_for_status()
                    ugoira_meta = hoordu.Dynamic.from_json(response.text).body
                    
                    orig = self._download_file(ugoira_meta.originalSrc)
                    
                    if file.update_metadata('frames', ugoira_meta.frames):
                        self.session.add(file)
                
                thumb = self._download_file(post.urls.small) if need_thumb else None
                
                self.session.import_file(file, orig=orig, thumb=thumb, move=True)
            
        elif post.pageCount == 1:
            # single page illust
            if len(remote_post.files) > 0:
                file = remote_post.files[0]
                
            else:
                file = File(remote=remote_post, remote_order=0)
                self.session.add(file)
                self.session.flush()
            
            need_orig = not file.present and not preview
            need_thumb = not file.thumb_present
            
            if need_thumb or need_orig:
                self.log.info(f'downloading file: {file.remote_order}')
                
                orig = self._download_file(post.urls.original) if need_orig else None
                thumb = self._download_file(post.urls.small) if need_thumb else None
                
                self.session.import_file(file, orig=orig, thumb=thumb, move=True)
            
        else:
            # multi page illust or manga
            available = set(range(post.pageCount))
            present = set(file.remote_order for file in remote_post.files)
            
            for order in available - present:
                file = File(remote=remote_post, remote_order=order)
                self.session.add(file)
                self.session.flush()
            
            response = self.http.get(POST_PAGES_URL.format(post_id=post_id))
            response.raise_for_status()
            pages = hoordu.Dynamic.from_json(response.text).body
            
            for file in remote_post.files:
                need_orig = not file.present and not preview
                need_thumb = not file.thumb_present
                
                if need_thumb or need_orig:
                    self.log.info(f'downloading file: {file.remote_order}')
                    
                    orig = self._download_file(pages[file.remote_order].urls.original) if need_orig else None
                    thumb = self._download_file(pages[file.remote_order].urls.small) if need_thumb else None
                    
                    self.session.import_file(file, orig=orig, thumb=thumb, move=True)
        
        return remote_post
    
    def download(self, id=None, remote_post=None, preview=False):
        if id is None and remote_post is None:
            raise ValueError('either id or remote_post must be passed')
        
        if remote_post is not None:
            id = remote_post.original_id
        
        response = self.http.get(POST_GET_URL.format(post_id=id))
        response.raise_for_status()
        post = hoordu.Dynamic.from_json(response.text)
        
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
    
    def subscription_repr(self, options):
        return '{}:{}'.format(options.method, options.user_id)
    
    def iterator(self, plugin, subscription=None, options=None):
        if subscription is not None:
            options = hoordu.Dynamic.from_json(subscription.options)
        
        if options.method == 'illusts':
            return IllustIterator(plugin, subscription=subscription, options=options)
            
        elif options.method == 'bookmarks':
            return BookmarkIterator(plugin, subscription=subscription, options=options)

Plugin = Pixiv


