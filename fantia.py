#!/usr/bin/env python3

import os
import re
from datetime import datetime, timezone
import dateutil.parser
from tempfile import mkstemp
import shutil
from urllib.parse import urlparse
import functools

import aiohttp
import contextlib
from bs4 import BeautifulSoup

import hoordu
from hoordu.models import *
from hoordu.plugins import *
from hoordu.forms import *
from hoordu.util import save_data_uri

POST_FORMAT = 'https://fantia.jp/posts/{post_id}'
POST_REGEXP = re.compile('^https?:\/\/fantia\.jp\/posts\/(?P<post_id>\d+)(?:\?.*)?(?:#.*)?$', flags=re.IGNORECASE)
FANCLUB_REGEXP = re.compile('^https?:\/\/fantia\.jp\/fanclubs\/(?P<fanclub_id>\d+)(?:\/.*)?(?:\?.*)?(?:#.*)?$', flags=re.IGNORECASE)
FILENAME_REGEXP = re.compile('^[a-z0-9]+-(?P<filename>.+)$')

POST_GET_URL = 'https://fantia.jp/api/v1/posts/{post_id}'
FANCLUB_URL = 'https://fantia.jp/fanclubs/{fanclub_id}'
FANCLUB_GET_URL = 'https://fantia.jp/api/v1/fanclubs/{fanclub_id}'
FILE_DOWNLOAD_URL = 'https://fantia.jp{download_uri}'

class CreatorIterator(IteratorBase['Fantia']):
    def __init__(self, fantia, subscription=None, options=None):
        super().__init__(fantia, subscription=subscription, options=options)
        
        self.http: aiohttp.ClientSession = fantia.http
        
        self.state.head_id = self.state.get('head_id')
        self.state.tail_id = self.state.get('tail_id')
    
    def __repr__(self):
        return 'posts:{}'.format(self.options.creator_id)
    
    def reconfigure(self, direction=FetchDirection.newer, num_posts=None):
        if self.state.tail_id is None:
            direction = FetchDirection.older
        
        super().reconfigure(direction=direction, num_posts=num_posts)
    
    async def _recover_head(self):
        async with self.http.get(FANCLUB_GET_URL.format(fanclub_id=self.options.creator_id)) as response:
            response.raise_for_status()
            fanclub = hoordu.Dynamic.from_json(await response.text()).fanclub
        
        if not fanclub.recent_posts:
            return
        
        post_id = fanclub.recent_posts[0].id
        head_id = post_id
        
        while post_id > self.state.head_id:
            sort_index = int(post_id)
            
            csrf = await self.plugin._get_csrf_token(post_id)
            async with self.http.get(POST_GET_URL.format(post_id=post_id), headers={'X-CSRF-Token': csrf}) as response:
                response.raise_for_status()
                post = hoordu.Dynamic.from_json(await response.text()).post
            
            yield sort_index, post
            
            next_post = post.links.previous
            if next_post is None:
                self.state.tail_id = post.id
                break
            
            post_id = next_post.id
        
        self.state.head_id = head_id
    
    async def _post_iterator(self):
        post_id = self.state.head_id if self.direction == FetchDirection.newer else self.state.tail_id
        
        if post_id is None:
            async with self.http.get(FANCLUB_GET_URL.format(fanclub_id=self.options.creator_id)) as response:
                response.raise_for_status()
                fanclub = hoordu.Dynamic.from_json(await response.text()).fanclub
            
            if not fanclub.recent_posts:
                return
            
            post_id = fanclub.recent_posts[0].id
            self.state.head_id = post_id
            self.state.tail_id = post_id
            
        else:
            post = None
            csrf = await self.plugin._get_csrf_token(post_id)
            headers = {
                'X-CSRF-Token': csrf,
                'X-Requested-With': 'XMLHttpRequest',
            }
            async with self.http.get(POST_GET_URL.format(post_id=post_id), headers=headers) as response:
                was_deleted = (response.status == 404)
                if not was_deleted:
                    response.raise_for_status()
                    post = hoordu.Dynamic.from_json(await response.text()).post
                
            if not was_deleted:
                next_post = post.links.next if self.direction == FetchDirection.newer else post.links.previous
                if next_post is None:
                    return
                
                post_id = next_post.id
                
            elif self.direction == FetchDirection.newer:
                async for sort_index, post in self._recover_head():
                    yield sort_index, post
                return
                
            else:
                raise Exception('tail post was deleted, recover functionality not implemented')
        
        # iter(int, 1) -> infinite iterator
        it = range(self.num_posts) if self.num_posts is not None else iter(int, 1)
        for _ in it:
            csrf = await self.plugin._get_csrf_token(post_id)
            headers = {
                'X-CSRF-Token': csrf,
                'X-Requested-With': 'XMLHttpRequest',
            }
            async with self.http.get(POST_GET_URL.format(post_id=post_id), headers=headers) as response:
                response.raise_for_status()
                post = hoordu.Dynamic.from_json(await response.text()).post
            
            sort_index = int(post_id)
            yield sort_index, post
            
            if self.direction == FetchDirection.newer:
                self.state.head_id = post_id
            elif self.direction == FetchDirection.older:
                self.state.tail_id = post_id
            
            next_post = post.links.next if self.direction == FetchDirection.newer else post.links.previous
            if next_post is None:
                break
            
            post_id = next_post.id
    
    async def generator(self):
        async for sort_index, post in self._post_iterator():
            remote_post = await self.plugin._to_remote_post(post, preview=self.subscription is None)
            yield remote_post
            
            if self.subscription is not None:
                await self.subscription.add_post(remote_post, sort_index)
            
            await self.session.commit()
        
        if self.subscription is not None:
            self.subscription.state = self.state.to_json()
            self.session.add(self.subscription)
        
        await self.session.commit()

class Fantia(SimplePlugin):
    name = 'fantia'
    version = 1
    iterator = CreatorIterator
    
    @classmethod
    def config_form(cls):
        return Form('{} config'.format(cls.name),
            ('session_id', Input('_session_id cookie', [validators.required]))
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
        
        match = FANCLUB_REGEXP.match(url)
        if match:
            return hoordu.Dynamic({
                'creator_id': match.group('fanclub_id')
            })
        
        return None
    
    @contextlib.asynccontextmanager
    async def context(self):
        async with super().context():
            self._headers = {
                'Origin': 'https://fantia.jp/',
                'Referer': 'https://fantia.jp/',
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:80.0) Gecko/20100101 Firefox/82.0'
            }
            self._cookies = {
                '_session_id': self.config.session_id
            }
            
            async with aiohttp.ClientSession(headers=self._headers, cookies=self._cookies) as http:
                self.http: aiohttp.ClientSession = http
                yield self
    
    async def _get_csrf_token(self, post_id):
        async with self.http.get(POST_FORMAT.format(post_id=post_id)) as response:
            response.raise_for_status()
            html = BeautifulSoup(await response.text(), 'html.parser')
            meta_tag = html.select('meta[name="csrf-token"]')[0]
            return meta_tag['content']
    
    async def _download_file(self, url, filename=None):
        if url.startswith('data:'):
            path = save_data_uri(url)
            
        else:
            path, resp = await self.session.download(url, headers=self._headers, cookies=self._cookies, suffix=filename)
        
        return path
    
    async def _content_to_post(self, post, content, remote_post=None, preview=False):
        content_id = '{post_id}-{content_id}'.format(post_id=post.id, content_id=content.id)
        creator_id = str(post.fanclub.id)
        creator_name = post.fanclub.user.name
        # possible timezone issues?
        post_time = dateutil.parser.parse(post.posted_at).astimezone(timezone.utc).replace(tzinfo=None)
        
        if remote_post is None:
            remote_post = await self._get_post(content_id)
        
        metadata = hoordu.Dynamic()
        if content.plan is not None:
            metadata.price = content.plan.price
        
        remote_post.url = POST_FORMAT.format(post_id=post.id)
        remote_post.title = content.title
        remote_post.comment = content.comment
        remote_post.type = PostType.collection
        remote_post.post_time = post_time
        remote_post.metadata_ = metadata.to_json()
        
        if post.liked is True:
            remote_post.favorite = True
        
        self.session.add(remote_post)
        
        self.log.info(f'downloading post: {remote_post.original_id}')
        self.log.info(f'local id: {remote_post.id}')
        
        # creators are identified by their id because their name can change
        creator_tag = await self._get_tag(TagCategory.artist, creator_id)
        await remote_post.add_tag(creator_tag)
        
        if creator_tag.update_metadata('name', creator_name):
            self.session.add(creator_tag)
        
        for tag in post.tags:
            remote_tag = await self._get_tag(TagCategory.general, tag.name)
            await remote_post.add_tag(remote_tag)
        
        if post.rating == 'adult':
            nsfw_tag = await self._get_tag(TagCategory.meta, 'nsfw')
            await remote_post.add_tag(nsfw_tag)
        
        files = await remote_post.fetch(RemotePost.files)
        if content.category == 'file':
            if len(files) == 0:
                file = File(remote=remote_post, remote_order=0, filename=content.filename)
                
                self.session.add(file)
                await self.session.flush()
                
            else:
                file = files[0]
            
            need_orig = not file.present and not preview
            
            if need_orig:
                self.log.info(f'downloading file: {content.filename}')
                
                orig_url = FILE_DOWNLOAD_URL.format(download_uri=content.download_uri)
                orig = await self._download_file(orig_url, filename=content.filename)
                
                await self.session.import_file(file, orig=orig, move=True)
            
        elif content.category == 'photo_gallery':
            current_files = {file.metadata_: file for file in files}
            
            order = 0
            for photo in content.post_content_photos:
                photo_id = str(photo.id)
                file = current_files.get(photo_id)
                
                if file is None:
                    file = File(remote=remote_post, metadata_=photo_id, remote_order=order)
                    self.session.add(file)
                    await self.session.flush()
                    
                elif file.remote_order != order:
                    file.remote_order = order
                    self.session.add(file)
                
                need_orig = not file.present and not preview
                need_thumb = not file.thumb_present
                
                if need_thumb or need_orig:
                    self.log.info(f'downloading file: {file.remote_order}')
                    
                    orig = await self._download_file(photo.url.original) if need_orig else None
                    thumb = await self._download_file(photo.url.medium) if need_thumb else None
                    
                    await self.session.import_file(file, orig=orig, thumb=thumb, move=True)
                
                order += 1
            
        elif content.category == 'text':
            # there are no files to save
            remote_post.type = PostType.set
            self.session.add(remote_post)
            
        elif content.category == 'blog':
            current_files = {file.remote_order: file for file in files}
            
            sections = hoordu.Dynamic.from_json(content.comment).ops
            blog = []
            order = 0
            for section in sections:
                insert = section.insert
                if isinstance(insert, str):
                    blog.append({
                        'type': 'text',
                        'content': insert
                    })
                    
                elif isinstance(insert, hoordu.Dynamic):
                    fantiaImage = insert.get('fantiaImage')
                    image = insert.get('image')
                    if fantiaImage is not None:
                        photo_id = str(fantiaImage.id)
                        file = current_files.get(photo_id)
                        
                        if file is None:
                            file = File(remote=remote_post, metadata_=photo_id, remote_order=order)
                            self.session.add(file)
                            await self.session.flush()
                        
                        if fantiaImage.url.startswith('data:'):
                            orig_url = fantiaImage.url
                            thumb_url = fantiaImage.url
                            
                        else:
                            # should use parse_url here (function from other plugins)
                            orig_url = FILE_DOWNLOAD_URL.format(download_uri=fantiaImage.original_url)
                            thumb_url = fantiaImage.url
                        
                        need_orig = not file.present and not preview
                        need_thumb = not file.thumb_present
                        
                        if need_thumb or need_orig:
                            self.log.info(f'downloading file: {file.remote_order}')
                            
                            orig = await self._download_file(orig_url) if need_orig else None
                            thumb = await self._download_file(thumb_url) if need_thumb else None
                            
                            await self.session.import_file(file, orig=orig, thumb=thumb, move=True)
                        
                        blog.append({
                            'type': 'file',
                            'metadata': photo_id
                        })
                        
                        order += 1
                        
                    elif image is not None:
                        image_id = '0:' + re.split('[\/.]', image)[-2]
                        file = current_files.get(image_id)
                        
                        if file is None:
                            file = File(remote=remote_post, metadata_=image_id, remote_order=order)
                            self.session.add(file)
                            await self.session.flush()
                        
                        need_orig = not file.present and not preview
                        
                        if need_orig:
                            self.log.info(f'downloading file: {file.remote_order}')
                            
                            orig = await self._download_file(image) if need_orig else None
                            
                            await self.session.import_file(file, orig=orig, move=True)
                        
                        blog.append({
                            'type': 'file',
                            'metadata': image_id
                        })
                        
                        order += 1
                        
                    else:
                        self.log.warning(f'unknown blog insert: {str(insert)}')
            
            remote_post.comment = hoordu.Dynamic({'comment': blog}).to_json()
            remote_post.type = PostType.blog
            self.session.add(remote_post)
            
        else:
            raise NotImplementedError('unknown content category: {}'.format(content.category))
        
        return remote_post
    
    async def _to_remote_post(self, post, remote_post=None, preview=False):
        main_id = str(post.id)
        creator_id = str(post.fanclub.id)
        creator_name = post.fanclub.user.name
        # possible timezone issues?
        post_time = dateutil.parser.parse(post.posted_at).astimezone(timezone.utc).replace(tzinfo=None)
        
        if remote_post is not None:
            id_parts = remote_post.id.split('-')
            if len(id_parts) == 2:
                content_id = int(id_parts[1])
                
                content = next((c for c in post.post_contents if c.id == content_id), None)
                
                if content is not None and content.visible_status == 'visible':
                    return [self._content_to_post(post, content, remote_post, preview)]
                else:
                    return [remote_post]
        
        if remote_post is None:
            remote_post = await self._get_post(main_id)
        
        remote_post.url = POST_FORMAT.format(post_id=main_id)
        remote_post.title = post.title
        remote_post.comment = post.comment
        remote_post.type = PostType.collection
        remote_post.post_time = post_time
        
        if post.liked is True:
            remote_post.favorite = True
        
        self.session.add(remote_post)
        
        self.log.info(f'downloading post: {remote_post.original_id}')
        self.log.info(f'local id: {remote_post.id}')
        
        # creators are identified by their id because their name can change
        creator_tag = await self._get_tag(TagCategory.artist, creator_id)
        await remote_post.add_tag(creator_tag)
        
        if creator_tag.update_metadata('name', creator_name):
            self.session.add(creator_tag)
        
        for tag in post.tags:
            remote_tag = await self._get_tag(TagCategory.general, tag.name)
            await remote_post.add_tag(remote_tag)
        
        if post.rating == 'adult':
            nsfw_tag = await self._get_tag(TagCategory.meta, 'nsfw')
            await remote_post.add_tag(nsfw_tag)
        
        # download thumbnail if there is one
        files = await remote_post.fetch(RemotePost.files)
        if len(files) == 0:
            if post.thumb is not None:
                file = File(remote=remote_post, remote_order=0)
                self.session.add(file)
                await self.session.flush()
            else:
                file = None
        else:
            file = files[0]
        
        if file is not None:
            need_orig = not file.present and not preview
            need_thumb = not file.thumb_present
            if need_orig or need_thumb:
                self.log.info(f'downloading file: {file.remote_order}')
                
                orig = await self._download_file(post.thumb.original) if need_orig else None
                thumb = await self._download_file(post.thumb.medium) if need_thumb else None
                
                await self.session.import_file(file, orig=orig, thumb=thumb, move=True)
        
        # convert the post contents to posts as well
        for content in post.post_contents:
            if content.visible_status == 'visible':
                content_post = await self._content_to_post(post, content, preview=preview)
                
                related = await remote_post.fetch(RemotePost.related)
                if not any(r.remote_id == content_post.id for r in related):
                    self.session.add(Related(related_to=remote_post, remote=content_post))
                
                await self.session.flush()
        
        return remote_post
    
    async def download(self, id=None, remote_post=None, preview=False):
        if id is None and remote_post is None:
            raise ValueError('either id or remote_post must be passed')
        
        if remote_post is not None:
            id = remote_post.original_id.split('-')[0]
        
        csrf = await self._get_csrf_token(id)
        headers = {
            'X-CSRF-Token': csrf,
            'X-Requested-With': 'XMLHttpRequest'
        }
        async with self.http.get(POST_GET_URL.format(post_id=id), headers=headers) as response:
            response.raise_for_status()
            post = hoordu.Dynamic.from_json(await response.text()).post
        
        remote_post = await self._to_remote_post(post, remote_post=remote_post, preview=preview)
        return remote_post
    
    def search_form(self):
        return Form('{} search'.format(self.name),
            ('creator_id', Input('fanclub id', [validators.required()]))
        )
    
    async def get_search_details(self, options):
        async with self.http.get(FANCLUB_URL.format(fanclub_id=options.creator_id)) as html_response:
            html_response.raise_for_status()
            html = BeautifulSoup(await html_response.text(), 'html.parser')
        
        async with self.http.get(FANCLUB_GET_URL.format(fanclub_id=options.creator_id)) as response:
            response.raise_for_status()
            fanclub = hoordu.Dynamic.from_json(await response.text()).fanclub
        
        related_urls = {x['href'] for x in html.select('main .btns:not(.share-btns) a')}
        
        return SearchDetails(
            hint=fanclub.user.name,
            title=fanclub.name,
            description=fanclub.comment,
            thumbnail_url=fanclub.icon.main,
            related_urls=related_urls
        )

Plugin = Fantia


