import re
import re
import itertools
import aiohttp
from datetime import datetime, timezone
import dateutil.parser
from natsort import natsorted

import hoordu
from hoordu.models import *
from hoordu.plugins import *
from hoordu.forms import *
from hoordu.oauth.client import *
from hoordu.http.requests import HTTPError


AUTH_URL = 'https://accounts.google.com/o/oauth2/auth'
TOKEN_URL = 'https://oauth2.googleapis.com/token'
REDIRECT_URL = 'http://127.0.0.1:8941/gdrive'
SCOPES = 'https://www.googleapis.com/auth/drive.readonly'

FOLDER_FORMAT = 'https://drive.google.com/drive/folders/{file_id}'
FILE_FORMAT = 'https://drive.google.com/file/d/{file_id}'
FILE_REGEXP = [
    re.compile('^https?:\/\/drive\.google\.com\/drive\/(u\/\d+\/)?folders\/(?P<file_id>[^\/\?]+)(?:\/.*)?(?:\?.*)?$', flags=re.IGNORECASE),
    re.compile('^https?:\/\/drive\.google\.com\/file\/d\/(?P<file_id>[^\/\?]+)(?:\/.*)?(?:\?.*)?$', flags=re.IGNORECASE)
]


class Drive:
    ENDPOINT = 'https://www.googleapis.com/drive/v3'
    PAGE_LIMIT = 100
    def __init__(self, access_token, refresh_token_cb):
        self.access_token = access_token
        self.refresh_token_cb = refresh_token_cb
        self.http = aiohttp.ClientSession()
        self.http.headers['Authorization'] = f'Bearer {access_token}'
    
    async def __aenter__(self) -> 'Drive':
        await self.http.__aenter__()
        return self
    
    async def __aexit__(self, *args):
        return await self.http.__aexit__(*args)
    
    @contextlib.asynccontextmanager
    async def _get(self, *args, **kwargs):
        async with self.http.get(*args, **kwargs) as resp:
            if resp.status == 401:
                self.access_token = await self.refresh_token_cb()
                
                self.http.headers['Authorization'] = f'Bearer {self.access_token}'
                
                async with self.http.get(*args, **kwargs) as resp_retry:
                    yield resp_retry
                    return
            
            yield resp
            return
    
    def is_link(self, f):
        return f.mimeType == 'application/vnd.google-apps.shortcut'
    
    def is_dir(self, f):
        return f.mimeType == 'application/vnd.google-apps.folder'
    
    async def folder(self, id):
        page_token = None
        
        while True:
            args = {
                'q': f"'{id}' in parents",
                'fields': 'nextPageToken, files(id, name, mimeType, createdTime, thumbnailLink, shortcutDetails)',
                'pageSize': self.PAGE_LIMIT
            }
            
            if page_token is not None:
                args['pageToken'] = page_token
            
            async with self._get(f'{self.ENDPOINT}/files', params=args) as resp:
                files = hoordu.Dynamic.from_json(await resp.text())
            
            for f in files.files:
                if self.is_link(f):
                    f.id = f.shortcutDetails.targetId
                    f.mimeType = f.shortcutDetails.targetMimeType
                
                yield f
            
            page_token = files.get('nextPageToken')
            
            if page_token is None:
                return
    
    async def file(self, id):
        args = {
            'fields': 'id, name, mimeType, createdTime, thumbnailLink, shortcutDetails'
        }
        async with self._get(f'{self.ENDPOINT}/files/{id}', params=args) as resp:
            return hoordu.Dynamic.from_json(await resp.text())
    
    def file_url(self, file):
        return f'{self.ENDPOINT}/files/{file.id}?alt=media'


class GDrive(SimplePlugin):
    name = 'gdrive'
    version = 1
    
    @classmethod
    def config_form(cls):
        return Form('{} config'.format(cls.name),
            ('client_id', Input('client id', [validators.required])),
            ('client_secret', Input('client secret', [validators.required])),
            ('access_token', Input('access token')),
            ('refresh_token', Input('refresh token'))
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
        
        if not config.contains('client_id', 'client_secret'):
            # but if they're still None, the api can't be used
            return False, cls.config_form()
        
        elif not config.contains('access_token', 'refresh_token'):
            code = None
            if parameters is not None:
                code = parameters.get('code')
            
            oauth = OAuth(**{
                'auth_url': AUTH_URL,
                'token_url': TOKEN_URL,
                'redirect_uri': REDIRECT_URL,
                'scopes': SCOPES,
                'client_id': config.client_id,
                'client_secret': config.client_secret
            })
            
            if code is None:
                url, _, _ = oauth.auth_url(extra_args={'access_type': 'offline'})
                
                return False, OAuthForm('google authentication', url)
                
            else:
                response = await oauth.get_access_token(code)
                config.access_token = response['access_token']
                config.refresh_token = response['refresh_token']
                plugin.config = config.to_json()
                session.add(plugin)
                
                return True, None
            
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
        for regexp in FILE_REGEXP:
            match = regexp.match(url)
            if match:
                return match.group('file_id')
        
        return None
    
    @contextlib.asynccontextmanager
    async def context(self):
        async with super().context():
            self.oauth = OAuth(**{
                'auth_url': AUTH_URL,
                'token_url': TOKEN_URL,
                'redirect_uri': REDIRECT_URL,
                'scopes': SCOPES,
                'client_id': self.config.client_id,
                'client_secret': self.config.client_secret
            })
            
            async with Drive(self.config.access_token, self._refresh_token) as api:
                self.api: Drive = api
                yield self
    
    async def _refresh_token(self):
        session = self.session.priority
        plugin = await self.get_plugin(session)
        config = hoordu.Dynamic.from_json(plugin.config)
        
        try:
            self.log.info('attempting to refresh access token')
            tokens = await self.oauth.refresh_access_token(config.refresh_token)
            
        except OAuthError as e:
            self.log.warning('refresh token was invalid')
            #msg = hoordu.Dynamic.from_json(str(e))
            
            # refresh token expired or revoked
            config.pop('access_token')
            config.pop('refresh_token')
            plugin.config = config.to_json()
            session.add(plugin)
            await session.commit()
            
            raise
        
        access_token = tokens['access_token']
        refresh_token = tokens.get('refresh_token')
        
        self.config.access_token = access_token
        config.access_token = access_token
        
        if refresh_token is not None:
            self.config.refresh_token = refresh_token
            config.refresh_token = refresh_token
        
        # update access_token in the database
        plugin.config = config.to_json()
        session.add(plugin)
        await session.commit()
        
        return access_token
    
    async def _ordered_walk(self, node, base_path=''):
        files = [f async for f in self.api.folder(node.id)]
        for n in natsorted(files, key=lambda x: (self.api.is_dir(x), x.name.lower())):
            path = base_path + n.name
            if not self.api.is_dir(n):
                yield path, n
            
            else:
                async for n in self._ordered_walk(n, base_path=path + '/'):
                    yield n
    
    async def _download_file(self, file):
        url = self.api.file_url(file)
        headers = {'Authorization': f'Bearer {self.config.access_token}'}
        
        try:
            path, _ = await self.session.download(url, headers=headers, suffix=file.name)
            return path
            
        except HTTPError as e:
            if e.status == 401:
                self.api.access_token = await self._refresh_token()
                headers = {'Authorization': f'Bearer {self.config.access_token}'}
                
                path, _ = await self.session.download(url, headers=headers, suffix=file.name)
                return path
    
    async def download(self, id=None, remote_post=None, preview=False):
        if id is None and remote_post is None:
            raise ValueError('either id or remote_post must be passed')
        
        if remote_post is not None:
            id = remote_post.original_id
        
        node = await self.api.file(id)
        
        original_id = node.id
        
        url = None
        if self.api.is_dir(node):
            url = FOLDER_FORMAT.format(file_id=node.id)
            
        else:
            url = FILE_FORMAT.format(file_id=node.id)
        
        create_time = dateutil.parser.parse(node.createdTime).astimezone(timezone.utc).replace(tzinfo=None)
        
        if remote_post is None:
            remote_post = await self._get_post(original_id)
        
        remote_post.title = node.name
        remote_post.url = url
        remote_post.type = PostType.set
        remote_post.post_time = create_time
        self.session.add(remote_post)
        
        self.log.info(f'downloading post: {remote_post.original_id}')
        self.log.info(f'local id: {remote_post.id}')
        
        files = await remote_post.fetch(RemotePost.files)
        current_files = {file.metadata_: file for file in files}
        
        if not self.api.is_dir(node):
            if len(files) == 0:
                file = File(remote=remote_post, remote_order=1, filename=node.name)
                self.session.add(file)
                await self.session.flush()
                
            else:
                file = files[0]
            
            need_orig = not file.present and not preview
            
            if need_orig:
                self.log.info(f'downloading file: {file.remote_order}')
                
                orig = await self._download_file(node)
                
                await self.session.import_file(file, orig=orig, move=True)
            
            return remote_post
        
        else:
            order = 0
            async for path, cnode in self._ordered_walk(node):
                order += 1
                
                id = cnode.id
                file = current_files.get(id)
                
                if file is None:
                    file = File(remote=remote_post, remote_order=order, filename=path, metadata_=id)
                    self.session.add(file)
                    await self.session.flush()
                    
                else:
                    file.filename = path
                    file.remote_order = order
                    self.session.add(file)
                
                need_orig = not file.present and not preview
                
                if need_orig:
                    self.log.info(f'downloading file: {file.remote_order}')
                    
                    orig = await self._download_file(cnode)
                    
                    await self.session.import_file(file, orig=orig, move=True)
            
            return remote_post

Plugin = GDrive


