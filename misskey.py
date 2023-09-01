import os
import re
from datetime import datetime, timezone
from tempfile import mkstemp
import shutil
from urllib.parse import urlparse, parse_qs
import functools
from collections import OrderedDict
import dateutil.parser

import aiohttp
import contextlib
import asyncio

import hoordu
from hoordu.models import *
from hoordu.plugins import *
from hoordu.forms import *


NOTE_FORMAT = 'https://misskey.io/notes/{note_id}'
NOTE_REGEXP = [
    re.compile('^https?:\/\/misskey\.io\/notes\/(?P<note_id>[a-z0-9]+)(?:\/.*)?(?:\?.*)?$', flags=re.IGNORECASE),
]
TIMELINE_REGEXP = re.compile('^https?:\/\/misskey\.io\/@(?P<user>[^\/]+)(?:\/(?P<type>[^\/]+)?)?(?:\?.*)?$', flags=re.IGNORECASE)

PAGE_LIMIT = 30


class MisskeyClient:
    def __init__(self, token):
        self.token = token
        self.http = aiohttp.ClientSession()
    
    async def __aenter__(self) -> 'MisskeyClient':
        await self.http.__aenter__()
        return self
    
    async def __aexit__(self, *args):
        return await self.http.__aexit__(*args)
    
    async def _request(self, *args, **kwargs):
        async with self.http.post(*args, **kwargs) as resp:
            resp.raise_for_status()
            return hoordu.Dynamic.from_json(await resp.text())
    
    async def get_user(self, handle):
        body = {
            'username': handle,
            'host': None,
            'i': self.token,
        }
        
        return await self._request('https://misskey.io/api/users/show', json=body)
    
    async def get_note(self, note_id):
        body = {
            'noteId': note_id,
            'i': self.token,
        }
        
        return await self._request('https://misskey.io/api/notes/show', json=body)
    
    async def get_timeline(self, user_id, count=PAGE_LIMIT, until_id=None):
        body = {
            'userId': user_id,
            'limit': count,
            'i': self.token,
            'excludeNsfw': False,
        }
        if until_id is not None:
            body['untilId'] = until_id
        
        return await self._request('https://misskey.io/api/users/notes', json=body)


class MisskeyIterator(IteratorBase['Misskey']):
    def __init__(self, plugin, subscription=None, options=None):
        super().__init__(plugin, subscription=subscription, options=options)
        
        self.api = plugin.api
        
        self.options.user_id = self.options.get('user_id')
        
        self.first_id = None
        self.state.head_id = self.state.get('head_id')
        self.state.tail_id = self.state.get('tail_id')
    
    def __repr__(self):
        return '{}:{}'.format(self.options.method, self.options.user_id)
    
    async def init(self):
        if self.options.user_id is None:
            user = await self.api.get_user(self.options.user)
            
            self.options.user_id = user.id
            
            if self.subscription is not None:
                self.subscription.options = self.options.to_json()
                self.session.add(self.subscription)
    
    def reconfigure(self, direction=FetchDirection.newer, num_posts=None):
        if direction == FetchDirection.newer:
            if self.state.tail_id is None:
                direction = FetchDirection.older
        
        num_posts = None
        
        super().reconfigure(direction=direction, num_posts=num_posts)
    
    async def _feed_iterator(self):
        until_id = None if self.direction == FetchDirection.newer else self.state.tail_id
        count = PAGE_LIMIT
        
        while True:
            self.log.info('getting next page')
            notes = await self.api.get_timeline(self.options.user_id, count=count, until_id=until_id)
            
            if len(notes) == 0:
                return
            
            for note in notes:
                yield note.id, note
                until_id = note.id
    
    def _validate_method(self, note):
        is_renote = False
        renote = note.get('renote')
        if renote is not None:
            is_renote = True
            note = renote
        
        media_list = note.get('files')
        
        has_files = (
            media_list is not None and
            len(media_list) > 0
        )
        
        if self.options.method == 'renotes':
            return has_files and is_renote
            
        elif self.options.method == 'notes':
            return has_files and not is_renote
    
    async def generator(self):
        is_first = True
        
        async for sort_index, note in self._feed_iterator():
            if is_first:
                if self.state.head_id is None or self.direction == FetchDirection.newer:
                    self.first_id = sort_index
                
                is_first = False
            
            if self.direction == FetchDirection.newer and sort_index <= self.state.head_id:
                break
            
            if self._validate_method(note):
                remote_post = await self.plugin._to_remote_post(note, preview=self.subscription is None)
                yield remote_post
                
                if self.subscription is not None:
                    await self.subscription.add_post(remote_post, int(sort_index, 36))
                
                await self.session.commit()
            
            if self.direction == FetchDirection.older:
                self.state.tail_id = sort_index
        
        if self.first_id is not None:
            self.state.head_id = self.first_id
            self.first_id = None
        
        if self.subscription is not None:
            self.subscription.state = self.state.to_json()
            self.session.add(self.subscription)
        
        await self.session.commit()


class Misskey(SimplePlugin):
    name = 'misskey'
    version = 1
    iterator = MisskeyIterator
    
    @classmethod
    def config_form(cls):
        return Form('{} config'.format(cls.name),
            ('token', Input('token', [validators.required])),
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
        
        if not config.contains('token'):
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
        
        for regexp in NOTE_REGEXP:
            match = regexp.match(url)
            if match:
                return match.group('note_id')
        
        match = TIMELINE_REGEXP.match(url)
        if match:
            user = match.group('user')
            method = match.group('type')
            
            method = 'notes'
            
            return hoordu.Dynamic({
                'user': user,
                'method': method
            })
        
        return None
    
    @contextlib.asynccontextmanager
    async def context(self):
        async with super().context():
            async with MisskeyClient(self.config.token) as api:
                self.api: MisskeyClient = api
                yield self
    
    async def _download_file(self, url, filename=None):
        u = urlparse(url)
        if u.netloc == 'proxy.misskeyusercontent.com':
            url = parse_qs(u.query)['url'][0]
        
        path, resp = await self.session.download(url, suffix=filename)
        return path
    
    async def _to_remote_post(self, note, remote_post=None, preview=False):
        renote = note.get('renote')
        if renote is not None and note.text is None and note.cw is None and len(note.files) == 0:
            self.log.info(f'downloading renote: {note.id}')
            note = renote
        
        original_id = note.id
        user = note.user.username if note.user.host is None else f'{note.user.username}@{note.user.host}'
        text = note.text if note.cw is None else f'{note.cw}\n{note.text}'
        post_time = dateutil.parser.isoparse(note.createdAt).replace(tzinfo=None)
        
        if remote_post is None:
            remote_post = await self._get_post(original_id)
        
        remote_post.url = NOTE_FORMAT.format(note_id=original_id)
        remote_post.comment = text
        remote_post.type = PostType.set
        remote_post.post_time = post_time
        remote_post.metadata_ = hoordu.Dynamic({'user': user}).to_json()
        self.session.add(remote_post)
        
        self.log.info(f'downloading post: {remote_post.original_id}')
        self.log.info(f'local id: {remote_post.id}')
        
        user_tag = await self._get_tag(TagCategory.artist, user)
        await remote_post.add_tag(user_tag)
        
        has_nsfw_file = any(f.isSensitive for f in note.files)
        if has_nsfw_file or note.cw is not None:
            nsfw_tag = await self._get_tag(TagCategory.meta, 'nsfw')
            await remote_post.add_tag(nsfw_tag)
        
        hashtags = note.get('tags')
        if hashtags is not None:
            for hashtag in hashtags:
                tag = await self._get_tag(TagCategory.general, hashtag)
                await remote_post.add_tag(tag)
        
        quoted = note.get('renote')
        if quoted is not None:
            await remote_post.add_related_url(NOTE_FORMAT.format(note_id=quoted.id))
        
        self.session.add(remote_post)
        
        files = note.files
        if len(files) > 0:
            current_files = {file.metadata_: file for file in await remote_post.fetch(RemotePost.files)}
            
            order = 0
            for rfile in files:
                rfile_id = rfile.id
                file = current_files.get(rfile_id)
                
                if file is None:
                    file = File(remote=remote_post, metadata_=rfile_id, remote_order=order)
                    self.session.add(file)
                    await self.session.flush()
                    
                elif file.remote_order != order:
                    file.remote_order = order
                    self.session.add(file)
                
                need_orig = not file.present and not preview
                need_thumb = not file.thumb_present and rfile.thumbnailUrl is not None
                
                if need_thumb or need_orig:
                    self.log.info(f'downloading file: {file.remote_order}')
                    
                    orig = await self._download_file(rfile.url) if need_orig else None
                    thumb = None
                    try:
                        if need_thumb:
                            thumb = await self._download_file(rfile.thumbnailUrl)
                    except:
                        self.log.exception('error while downloading thumbnail')
                    
                    await self.session.import_file(file, orig=orig, thumb=thumb, move=True)
                
                order += 1
        
        return remote_post
    
    async def download(self, id=None, remote_post=None, preview=False):
        if id is None and remote_post is None:
            raise ValueError('either id or remote_post must be passed')
        
        if remote_post is not None:
            id = remote_post.original_id
        
        note = await self.api.get_note(id)
        
        return await self._to_remote_post(note, remote_post=remote_post, preview=preview)
    
    def search_form(self):
        return Form('{} search'.format(self.name),
            ('method', ChoiceInput('method', [
                    ('notes', 'notes'),
                    ('renotes', 'renotes'),
                ], [validators.required()])),
            ('user', Input('screen name', [validators.required()]))
        )
    
    async def get_search_details(self, options):
        user = await self.api.get_user(options.user)
        options.user_id = user.id
        
        related_urls = {}
        if user.description is not None:
            # find urls from description
            pass
        
        thumb_url = user.avatarUrl
        
        return SearchDetails(
            hint=user.username,
            title=user.name if user.name is not None else user.username,
            description=user.description,
            thumbnail_url=thumb_url,
            related_urls=related_urls
        )

Plugin = Misskey


