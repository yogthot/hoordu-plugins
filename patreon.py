#!/usr/bin/env python3

import os
import re
from datetime import datetime, timezone
import dateutil.parser
from tempfile import mkstemp
import shutil
from urllib.parse import urlparse
import itertools
import functools
import asyncio

import aiohttp
import contextlib
from bs4 import BeautifulSoup
from collections import OrderedDict

import hoordu
from hoordu.models import *
from hoordu.plugins import *
from hoordu.forms import *
from hoordu.plugins.helpers import parse_href

POST_FORMAT = 'https://www.patreon.com/posts/{post_id}'
POST_REGEXP = re.compile('^https?:\/\/(?:www\.)?patreon\.com\/posts\/(:?[^\?#\/]*-)?(?P<post_id>\d+)(?:\?.*)?(?:#.*)?$', flags=re.IGNORECASE)

CREATOR_REGEXP = re.compile('^https?:\/\/(?:www\.)?patreon\.com\/(?P<vanity>[^\/]+)(?:\/.*)?(?:\?.*)?(?:#.*)?$', flags=re.IGNORECASE)

POST_GET_URL = 'https://www.patreon.com/api/posts/{post_id}'
CREATOR_GET_BY_VANITY_URL = 'https://www.patreon.com/api/users'
CREATOR_GET_URL = 'https://www.patreon.com/api/user/{creator_id}'

POST_LIST_URL = 'https://www.patreon.com/api/posts'

class IncludedMap(OrderedDict):
    def __init__(self, included_raw):
        super().__init__()
        for include in included_raw:
            self[(include.type, include.id)] = include
    
    def __getitem__(self, key):
        if isinstance(key, tuple):
            return super().__getitem__(key)
            
        else:
            return super().__getitem__((key.type, key.id))


class CreatorIterator(IteratorBase['Patreon']):
    def __init__(self, plugin, subscription=None, options=None):
        super().__init__(plugin, subscription=subscription, options=options)
        
        self.http: aiohttp.ClientSession = plugin.http
        
        self.first_timestamp = None
        self.state.head_timestamp = self.state.get('head_timestamp')
        
        self.downloaded = set()
        self.cached_page = None
        
        if self.state.head_timestamp is None:
            self.head_timestamp = None
            
        else:
            self.head_timestamp = dateutil.parser.parse(self.state.head_timestamp)
    
    def __repr__(self):
        return 'posts:{}'.format(self.options.creator_id)
    
    def reconfigure(self, direction=FetchDirection.newer, num_posts=None):
        direction = FetchDirection.newer
        num_posts = None
        
        super().reconfigure(direction=direction, num_posts=num_posts)
    
    async def init(self):
        creator_resp = await self.plugin._get_creator(self.options.vanity)
        creator = creator_resp.data.attributes
        
        self.options.creator_id = creator_resp.data.id
        self.options.vanity = creator.vanity
        
        for incl in creator_resp.included:
            if incl.type == 'campaign':
                self.options.campaign_id = incl.id
                break
    
    async def _get_page(self, cursor=None):
        params = {
            'include': 'attachments,audio,images,media,user,user_defined_tags',
            'filter[campaign_id]': self.options.campaign_id,
            # what does this do?
            'filter[contains_exclusive_posts]': 'true',
            'filter[is_draft]': 'false',
            'sort': '-published_at',
            'json-api-use-default-includes': 'false',
            'json-api-version': '1.0',
        }
        
        if cursor is not None:
            params['page[cursor]'] = cursor
        
        self.log.info('getting next page')
        async with self.http.get(POST_LIST_URL, params=params) as response:
            response.raise_for_status()
            return hoordu.Dynamic.from_json(await response.text())
    
    async def _post_iterator(self):
        cursor = None
        
        use_cached = self.cached_page is not None
        
        while True:
            if not use_cached:
                page = await self._get_page(cursor)
                self.cached_page = page
            else:
                self.log.info('using cached page')
                page = self.cached_page
            
            use_cached = False
            
            includes = IncludedMap(page.included)
            
            if cursor is None and len(page.data) > 0:
                self.first_timestamp = page.data[0].attributes.published_at
            
            for post in page.data:
                sort_index = int(post.id)
                
                published_at = dateutil.parser.parse(post.attributes.published_at)
                if self.head_timestamp is not None and published_at < self.head_timestamp:
                    return
                
                if post.attributes.current_user_can_view:
                    yield sort_index, post, includes
            
            cursors = page.meta.pagination.get('cursors')
            if cursors is None:
                return
            
            cursor = cursors.next
            if cursor is None:
                return
    
    async def generator(self):
        async for sort_index, post, included in self._post_iterator():
            if post.id in self.downloaded:
                continue
            
            remote_post = await self.plugin._to_remote_post(post, included, preview=self.subscription is None)
            yield remote_post
            self.downloaded.add(post.id)
            
            if self.subscription is not None:
                await self.subscription.add_post(remote_post, sort_index)
            
            await self.session.commit()
        
        if self.first_timestamp is not None:
            self.state.head_timestamp = self.first_timestamp
            self.head_timestamp = dateutil.parser.parse(self.state.head_timestamp)
            self.first_timestamp = None
        
        if self.subscription is not None:
            self.subscription.state = self.state.to_json()
            self.session.add(self.subscription)
        
        await self.session.commit()

class Patreon(SimplePlugin):
    name = 'patreon'
    version = 1
    iterator = CreatorIterator
    
    @classmethod
    def config_form(cls):
        return Form('{} config'.format(cls.name),
            ('session_id', Input('session_id cookie', [validators.required]))
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
        
        if not config.contains('session_id'):
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
        
        match = POST_REGEXP.match(url)
        if match:
            return match.group('post_id')
        
        match = CREATOR_REGEXP.match(url)
        if match:
            return hoordu.Dynamic({
                'vanity': match.group('vanity')
            })
        
        return None
    
    @contextlib.asynccontextmanager
    async def context(self):
        async with super().context():
            self._headers = {
                'Origin': 'https://www.patreon.com/',
                'Referer': 'https://www.patreon.com/',
                'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64; rv:109.0) Gecko/20100101 Firefox/109.0',
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8',
                'Accept-Language': 'en-US,en;q=0.5',
                'Accept-Encoding': 'gzip, deflate, br',
                'DNT': '1',
                'Sec-Fetch-Dest': 'empty',
                'Sec-Fetch-Mode': 'no-cors',
                'Sec-Fetch-Site': 'same-origin',
                'TE': 'trailers',
            }
            self._cookies = {
                'session_id': self.config.session_id
            }
            
            async with aiohttp.ClientSession(headers=self._headers, cookies=self._cookies) as http:
                self.http: aiohttp.ClientSession = http
                yield self
    
    async def _get_creator(self, vanity):
        params = {
            'filter[vanity]': vanity,
            'json-api-use-default-includes': 'true',
            'json-api-version': '1.0'
        }
        
        async with self.http.get(CREATOR_GET_BY_VANITY_URL, params=params) as response:
            response.raise_for_status()
            return hoordu.Dynamic.from_json(await response.text())
    
    async def _download_file(self, url, filename=None):
        path, resp = await self.session.download(url, headers=self._headers, cookies=self._cookies, suffix=filename)
        await asyncio.sleep(1)
        return path
    
    async def _to_remote_post(self, post_obj, included, remote_post=None, preview=False):
        post = post_obj.attributes
        post_id = post_obj.id
        
        user = included[post_obj.relationships.user.data]
        user_id = user.id
        user_vanity = user.attributes.vanity
        
        post_time = dateutil.parser.parse(post.published_at).astimezone(timezone.utc).replace(tzinfo=None)
        
        if remote_post is None:
            remote_post = await self._get_post(post_id)
        
        remote_post.url = POST_FORMAT.format(post_id=post_id)
        remote_post.title = post.title
        remote_post.type = PostType.collection
        remote_post.post_time = post_time
        
        self.log.info(f'downloading post: {remote_post.original_id}')
        self.log.info(f'local id: {remote_post.id}')
        
        # parse post content
        urls = []
        if post.content is not None:
            content = re.sub('\s+', ' ', post.content)
            comment_html = BeautifulSoup(content, 'html.parser')
            
            page_url = POST_FORMAT.format(post_id=post_id)
            for a in comment_html.select('a'):
                url = parse_href(page_url, a['href'])
                urls.append(url)
                
                a.replace_with(url)
            
            for br in comment_html.find_all('br'):
                br.replace_with('\n')
            
            remote_post.comment = comment_html.text
        
        if post.current_user_has_liked is True:
            remote_post.favorite = True
        
        self.session.add(remote_post)
        
        creator_tag = await self._get_tag(TagCategory.artist, user_id)
        await remote_post.add_tag(creator_tag)
        
        if creator_tag.update_metadata('vanity', user_vanity):
            self.session.add(creator_tag)
        
        tags = post_obj.relationships.user_defined_tags.data or []
        for tag in tags:
            name = tag.id.split(';', 1)[1]
            remote_tag = await self._get_tag(TagCategory.general, name)
            await remote_post.add_tag(remote_tag)
        
        for url in urls:
            await remote_post.add_related_url(url)
        
        embed = post.get('embed')
        if embed is not None:
            await remote_post.add_related_url(embed.url)
        
        current_files = {file.metadata_: file for file in await remote_post.fetch(RemotePost.files)}
        
        images = post_obj.relationships.images.data or []
        attachments = post_obj.relationships.attachments.data or []
        
        audio = []
        audio_data = post_obj.relationships.audio.data
        if audio_data is not None:
            audio = [audio_data]
        
        for data, order in zip(itertools.chain(images, audio, attachments), itertools.count(1)):
            id = f'{data.type}-{data.id}'
            attributes = included[data].attributes
            
            orig_filename = None
            orig_url = None
            thumb_url = None
            
            if data.type == 'attachment':
                orig_filename = attributes.name
                orig_url = attributes.url
                
            elif data.type == 'media':
                # skip not ready images for now
                if attributes.state != 'ready':
                    continue
                
                # skip embeded image, url has been saved instead
                if post.post_type == 'link' and attributes.owner_relationship == 'main':
                    continue
                
                orig_url = attributes.image_urls.original
                thumb_url = attributes.image_urls.default
                
                orig_filename = attributes.file_name
            
            file = current_files.get(id)
            
            if file is None:
                file = File(remote=remote_post, remote_order=order, metadata_=id)
                self.session.add(file)
                await self.session.flush()
                
            else:
                file.remote_order = order
                self.session.add(file)
            
            need_orig = not file.present and orig_url is not None and not preview
            need_thumb = not file.thumb_present and thumb_url is not None
            
            # handle stupid url filenames
            if '/' in orig_filename:
                orig_filename = None
            
            if need_thumb or need_orig:
                self.log.info(f'downloading file: {file.remote_order}')
                
                orig = await self._download_file(orig_url, filename=orig_filename) if need_orig else None
                thumb = await self._download_file(thumb_url) if need_thumb else None
                
                await self.session.import_file(file, orig=orig, thumb=thumb, move=True)
        
        return remote_post
    
    async def download(self, id=None, remote_post=None, preview=False):
        if id is None and remote_post is None:
            raise ValueError('either id or remote_post must be passed')
        
        if remote_post is not None:
            id = remote_post.original_id
        
        params = {
            'include': 'attachments,audio,images,media,user,user_defined_tags',
            'json-api-use-default-includes': 'false',
            'json-api-version': '1.0'
        }
        
        async with self.http.get(POST_GET_URL.format(post_id=id), params=params) as response:
            response.raise_for_status()
            json = hoordu.Dynamic.from_json(await response.text())
        
        included = IncludedMap(json.included)
        return await self._to_remote_post(json.data, included, remote_post=remote_post, preview=preview)
    
    
    def search_form(self):
        return Form('{} search'.format(self.name),
            ('creator', Input('creator vanity', [validators.required()]))
        )
    
    async def get_search_details(self, options):
        creator_resp = await self._get_creator(options.vanity)
        creator = creator_resp.data.attributes
        
        options.creator_id = creator_resp.data.id
        
        for incl in creator_resp.included:
            if incl.type == 'campaign':
                options.campaign_id = incl.id
                break
        
        related_urls = set()
        for social in creator.social_connections.values():
            if social is not None and social.url is not None:
                related_urls.add(social.url)
        
        return SearchDetails(
            hint=creator.vanity,
            title=creator.full_name,
            description=creator.about,
            thumbnail_url=creator.image_url,
            related_urls=related_urls
        )

Plugin = Patreon


