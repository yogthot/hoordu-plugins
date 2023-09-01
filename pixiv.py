#!/usr/bin/env python3

import re
from datetime import datetime, timedelta, timezone
import dateutil.parser
from urllib.parse import unquote
from xml.sax.saxutils import unescape

import aiohttp
import contextlib
from bs4 import BeautifulSoup

import hoordu
from hoordu.models import *
from hoordu.plugins import *
from hoordu.forms import *
from hoordu.plugins.helpers import parse_href

POST_FORMAT = 'https://www.pixiv.net/artworks/{post_id}'
FANBOX_URL_FORMAT = 'https://www.pixiv.net/fanbox/creator/{user_id}'
POST_REGEXP = [
    re.compile('^(?P<post_id>\d+)_p\d+\.[a-zA-Z0-9]+$'),
    re.compile('^https?:\/\/(?:www\.)?pixiv\.net\/(?:[a-zA-Z]{2}\/)?artworks\/(?P<post_id>\d+)(?:\?.*)?(?:#.*)?$', flags=re.IGNORECASE)
]
USER_REGEXP = [
    re.compile('^https?:\/\/(?:www\.)?pixiv\.net\/(?:[a-zA-Z]{2}\/)?users\/(?P<user_id>\d+)(?:\/illustration|\/manga)?(?:\?.*)?(?:#.*)?$', flags=re.IGNORECASE)
]
BOOKMARKS_REGEXP = [
    re.compile('^https?:\/\/(?:www\.)?pixiv\.net\/(?:[a-zA-Z]{2}\/)?users\/(?P<user_id>\d+)\/bookmarks\/artworks(?:\?.*)?(?:#.*)?$', flags=re.IGNORECASE)
]
REDIRECT_REGEXP = re.compile('^https?:\/\/(?:www\.)?pixiv\.net\/jump\.php\?(?P<url>.*)$', flags=re.IGNORECASE)

USER_URL = 'https://www.pixiv.net/en/users/{user_id}'
POST_GET_URL = 'https://www.pixiv.net/ajax/illust/{post_id}'
POST_PAGES_URL = 'https://www.pixiv.net/ajax/illust/{post_id}/pages'
POST_UGOIRA_URL = 'https://www.pixiv.net/ajax/illust/{post_id}/ugoira_meta'
USER_POSTS_URL = 'https://www.pixiv.net/ajax/user/{user_id}/profile/all'
USER_BOOKMARKS_URL = 'https://www.pixiv.net/ajax/user/{user_id}/illusts/bookmarks'
BOOKMARKS_LIMIT = 48

class IllustIterator(IteratorBase['Pixiv']):
    def __init__(self, pixiv, subscription=None, options=None):
        super().__init__(pixiv, subscription=subscription, options=options)
        
        self.http: aiohttp.ClientSession = pixiv.http
        
        self.state.head_id = self.state.get('head_id')
        self.state.tail_id = self.state.get('tail_id')
    
    def __repr__(self):
        return '{}:{}'.format(self.options.method, self.options.user_id)
    
    async def _iterator(self):
        async with self.http.get(USER_POSTS_URL.format(user_id=self.options.user_id)) as resp:
            resp.raise_for_status()
            user_info = hoordu.Dynamic.from_json(await resp.text())
        
        if user_info.error is True:
            raise APIError(user_info.message)
        
        body = user_info.body
        
        posts = []
        for bucket in ('illusts', 'manga'):
            # these are [] when empty
            if isinstance(body[bucket], dict):
                posts.extend([int(id) for id in body[bucket].keys()])
        
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
            sort_index = int(post_id)
            async with self.http.get(POST_GET_URL.format(post_id=post_id)) as resp:
                resp.raise_for_status()
                post = hoordu.Dynamic.from_json(await resp.text())
            
            if post.error is True:
                raise APIError(post.message)
            
            if self.state.head_id is None:
                self.state.head_id = post_id
            
            remote_post = await self.plugin._to_remote_post(post.body, preview=self.subscription is None)
            yield sort_index, remote_post
            
            if self.direction == FetchDirection.newer:
                self.state.head_id = post_id
            elif self.direction == FetchDirection.older:
                self.state.tail_id = post_id
    
    async def generator(self):
        async for sort_index, post in self._iterator():
            yield post
            
            if self.subscription is not None:
                await self.subscription.add_post(post, sort_index)
            
            await self.session.commit()
        
        if self.subscription is not None:
            self.subscription.state = self.state.to_json()
            self.session.add(self.subscription)
        
        await self.session.commit()

class BookmarkIterator(IteratorBase['Pixiv']):
    def __init__(self, pixiv, subscription=None, options=None):
        super().__init__(pixiv, subscription=subscription, options=options)
        
        self.http: aiohttp.ClientSession = pixiv.http
        
        self.first_id = None
        self.state.head_id = self.state.get('head_id')
        self.state.tail_id = self.state.get('tail_id')
        self.state.offset = self.state.get('offset', 0)
    
    def __repr__(self):
        return '{}:{}'.format(self.options.method, self.options.user_id)
    
    def reconfigure(self, direction=FetchDirection.newer, num_posts=None):
        if direction == FetchDirection.newer:
            if self.state.tail_id is None:
                direction = FetchDirection.older
            else:
                num_posts = None
        
        super().reconfigure(direction=direction, num_posts=num_posts)
    
    async def _iterator(self):
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
            
            async with self.http.get(USER_BOOKMARKS_URL.format(user_id=self.options.user_id), params=params) as resp:
                resp.raise_for_status()
                bookmarks_resp = hoordu.Dynamic.from_json(await resp.text())
            
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
                
                async with self.http.get(POST_GET_URL.format(post_id=post_id)) as resp:
                    # skip this post if 404 (deleted bookmarks)
                    was_deleted = (resp.status == 404)
                    
                    if not was_deleted:
                        resp.raise_for_status()
                        post = hoordu.Dynamic.from_json(await resp.text())
                        
                        if post.error is True:
                            raise APIError(post.message)
                        
                        remote_post = await self.plugin._to_remote_post(post.body, preview=self.subscription is None)
                        yield bookmark_id, remote_post
                
                if self.direction == FetchDirection.older:
                    self.state.tail_id = bookmark.bookmarkData.id
                    self.state.offset += 1
                
                if not was_deleted:
                    total +=1
                    
                    if self.num_posts is not None and total >= self.num_posts:
                        return
            
            first_iteration = False
    
    async def generator(self):
        async for sort_index, post in self._iterator():
            yield post
            
            if self.subscription is not None:
                await self.subscription.add_post(post, sort_index)
            
            await self.session.commit()
        
        if self.first_id is not None:
            self.state.head_id = self.first_id
            self.first_id = None
        
        if self.subscription is not None:
            self.subscription.state = self.state.to_json()
            self.session.add(self.subscription)
        
        await self.session.commit()

class Pixiv(SimplePlugin):
    name = 'pixiv'
    version = 1
    
    @classmethod
    def config_form(cls):
        return Form('{} config'.format(cls.name),
            ('PHPSESSID', Input('PHPSESSID cookie', [validators.required]))
        )
    
    @classmethod
    async def setup(cls, session, parameters=None):
        plugin = await cls.get_plugin(session)
        
        # check if everything is ready to use
        config = hoordu.Dynamic.from_json(plugin.config)
        
        # use values from the parameters if they were passed
        if parameters is not None:
            config.update(parameters)
            
            plugin.config = config.to_json()
            session.add(plugin)
        
        if not config.contains('PHPSESSID'):
            # but if they're still None, the api can't be used
            return False, cls.config_form()
            
        else:
            # the config contains every required property
            return True, None
    
    @classmethod
    async def update(cls, session):
        plugin = await cls.get_plugin(session)
        
        if plugin.version < cls.version:
            # update anything if needed
            
            # if anything was updated, then the db entry should be updated as well
            plugin.version = cls.version
            session.add(plugin)
    
    @classmethod
    async def parse_url(cls, url):
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
    
    @contextlib.asynccontextmanager
    async def context(self):
        async with super().context():
            self._headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:80.0) Gecko/20100101 Firefox/82.0'
            }
            self._cookies = {
                'PHPSESSID': self.config.PHPSESSID
            }
            
            async with aiohttp.ClientSession(headers=self._headers, cookies=self._cookies) as http:
                self.http: aiohttp.ClientSession = http
                yield self
    
    async def _download_file(self, url):
        headers = dict(self._headers)
        headers['Referer'] = 'https://www.pixiv.net/'
        
        path, resp = await self.session.download(url, headers=headers, cookies=self._cookies)
        return path
    
    async def _to_remote_post(self, post, remote_post=None, preview=False):
        post_id = post.id
        user_id = post.userId
        user_name = post.userName
        user_account = post.userAccount
        # possible timezone issues?
        post_time = dateutil.parser.parse(post.createDate).astimezone(timezone.utc).replace(tzinfo=None)
        
        if post.illustType == 1:
            post_type = PostType.collection
        else:
            post_type = PostType.set
        
        if remote_post is None:
            remote_post = await self._get_post(post_id)
        
        remote_post.url = POST_FORMAT.format(post_id=post_id)
        remote_post.title = post.title
        remote_post.type = post_type
        remote_post.post_time = post_time
        
        self.log.info(f'downloading post: {remote_post.original_id}')
        self.log.info(f'local id: {remote_post.id}')
        
        # there is no visual difference in multiple whitespace (or newlines for that matter)
        # unless inside <pre>, but that's too hard to deal with :(
        description = re.sub('\s+', ' ', post.description)
        comment_html = BeautifulSoup(description, 'html.parser')
        
        urls = []
        page_url = POST_FORMAT.format(post_id=post_id)
        for a in comment_html.select('a'):
            url = parse_href(page_url, a['href'])
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
        
        self.session.add(remote_post)
        
        user_tag = await self._get_tag(TagCategory.artist, user_id)
        await remote_post.add_tag(user_tag)
        
        if any((user_tag.update_metadata('name', user_name),
                user_tag.update_metadata('account', user_account))):
            self.session.add(user_tag)
        
        for tag in post.tags.tags:
            remote_tag = await self._get_tag(TagCategory.general, tag.tag)
            await remote_post.add_tag(remote_tag)
            
            if tag.contains('romaji') and remote_tag.update_metadata('romaji', tag.romaji):
                self.session.add(remote_tag)
        
        if post.xRestrict >= 1:
            nsfw_tag = await self._get_tag(TagCategory.meta, 'nsfw')
            await remote_post.add_tag(nsfw_tag)
            
        if post.xRestrict >= 2:
            nsfw_tag = await self._get_tag(TagCategory.meta, 'extreme')
            await remote_post.add_tag(nsfw_tag)
        
        if post.isOriginal:
            original_tag = await self._get_tag(TagCategory.copyright, 'original')
            await remote_post.add_tag(original_tag)
        
        for url in urls:
            await remote_post.add_related_url(url)
        
        files = await remote_post.fetch(RemotePost.files)
        # files
        if post.illustType == 2:
            # ugoira
            if len(files) > 0:
                file = files[0]
                
            else:
                file = File(remote=remote_post, remote_order=0)
                self.session.add(file)
                await self.session.flush()
            
            need_orig = not file.present and not preview
            need_thumb = not file.thumb_present
            
            if need_thumb or need_orig:
                self.log.info(f'downloading file: {file.remote_order}')
                
                orig = None
                if need_orig:
                    async with self.http.get(POST_UGOIRA_URL.format(post_id=post_id)) as resp:
                        resp.raise_for_status()
                        ugoira_meta = hoordu.Dynamic.from_json(await resp.text()).body
                    
                    orig = await self._download_file(ugoira_meta.originalSrc)
                    
                    if file.update_metadata('frames', ugoira_meta.frames):
                        self.session.add(file)
                
                thumb = await self._download_file(post.urls.small) if need_thumb else None
                
                await self.session.import_file(file, orig=orig, thumb=thumb, move=True)
            
        elif post.pageCount == 1:
            # single page illust
            if len(files) > 0:
                file = files[0]
                
            else:
                file = File(remote=remote_post, remote_order=0)
                self.session.add(file)
                await self.session.flush()
            
            need_orig = not file.present and not preview
            need_thumb = not file.thumb_present
            
            if need_thumb or need_orig:
                self.log.info(f'downloading file: {file.remote_order}')
                
                orig = await self._download_file(post.urls.original) if need_orig else None
                thumb = await self._download_file(post.urls.small) if need_thumb else None
                
                await self.session.import_file(file, orig=orig, thumb=thumb, move=True)
            
        else:
            # multi page illust or manga
            available = set(range(post.pageCount))
            present = set(file.remote_order for file in files)
            
            for order in available - present:
                file = File(remote=remote_post, remote_order=order)
                self.session.add(file)
                await self.session.flush()
            
            async with self.http.get(POST_PAGES_URL.format(post_id=post_id)) as resp:
                resp.raise_for_status()
                pages = hoordu.Dynamic.from_json(await resp.text()).body
            
            files = await remote_post.fetch(RemotePost.files)
            for file in files:
                need_orig = not file.present and not preview
                need_thumb = not file.thumb_present
                
                if need_thumb or need_orig:
                    self.log.info(f'downloading file: {file.remote_order}')
                    
                    orig = await self._download_file(pages[file.remote_order].urls.original) if need_orig else None
                    thumb = await self._download_file(pages[file.remote_order].urls.small) if need_thumb else None
                    
                    await self.session.import_file(file, orig=orig, thumb=thumb, move=True)
        
        return remote_post
    
    async def download(self, id=None, remote_post=None, preview=False):
        if id is None and remote_post is None:
            raise ValueError('either id or remote_post must be passed')
        
        if remote_post is not None:
            id = remote_post.original_id
        
        async with self.http.get(POST_GET_URL.format(post_id=id)) as resp:
            resp.raise_for_status()
            post = hoordu.Dynamic.from_json(await resp.text())
        
        if post.error is True:
            self.log.error('pixiv api error: %s', post.message)
            return None
        
        return await self._to_remote_post(post.body, remote_post=remote_post, preview=preview)
    
    def search_form(self):
        return Form('{} search'.format(self.name),
            ('method', ChoiceInput('method', [
                    ('illusts', 'illustrations'),
                    ('bookmarks', 'bookmarks')
                ], [validators.required()])),
            ('user_id', Input('user id', [validators.required()]))
        )
    
    async def get_search_details(self, options):
        async with self.http.get(USER_URL.format(user_id=options.user_id)) as resp:
            resp.raise_for_status()
            html = BeautifulSoup(await resp.text(), 'html.parser')
        
        preload_json = html.select('#meta-preload-data')[0]['content']
        preload = hoordu.Dynamic.from_json(unescape(preload_json))
        
        user = preload.user[str(options.user_id)]
        
        related_urls = set()
        if user.webpage:
            related_urls.add(user.webpage)
        
        # it's [] when it's empty
        if isinstance(user.social, dict):
            related_urls.update(s.url for s in user.social.values())
        
        comment_html = BeautifulSoup(user.commentHtml, 'html.parser')
        related_urls.update(a.text for a in comment_html.select('a'))
        
        async with self.http.get(FANBOX_URL_FORMAT.format(user_id=options.user_id), allow_redirects=False) as creator_response:
            if creator_response.status // 100 == 3:
                related_urls.add(creator_response.headers['Location'])
        
        return SearchDetails(
            hint=user.name,
            title=user.name,
            description=user.comment,
            thumbnail_url=user.imageBig,
            related_urls=related_urls
        )
    
    def iterator(self, plugin, subscription=None, options=None):
        if subscription is not None:
            options = hoordu.Dynamic.from_json(subscription.options)
        
        if options.method == 'illusts':
            return IllustIterator(plugin, subscription=subscription, options=options)
            
        elif options.method == 'bookmarks':
            return BookmarkIterator(plugin, subscription=subscription, options=options)

Plugin = Pixiv


