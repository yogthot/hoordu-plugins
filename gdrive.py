import re
import itertools
import requests
from datetime import datetime, timezone
import dateutil.parser
from natsort import natsorted

import hoordu
from hoordu.models import *
from hoordu.plugins import *
from hoordu.forms import *
from hoordu.plugins.oauth import *

AUTH_URL = 'https://accounts.google.com/o/oauth2/auth'
TOKEN_URL = 'https://oauth2.googleapis.com/token'
REDIRECT_URL = 'urn:ietf:wg:oauth:2.0:oob'
SCOPES = 'https://www.googleapis.com/auth/drive.readonly'

FOLDER_FORMAT = 'https://drive.google.com/drive/folders/{file_id}'
FILE_FORMAT = 'https://drive.google.com/file/d/{file_id}'
FILE_REGEXP = [
    re.compile('^https?:\/\/drive\.google\.com\/drive\/folders\/(?P<file_id>[^\/\?]+)(?:\/.*)?(?:\?.*)?$', flags=re.IGNORECASE),
    re.compile('^https?:\/\/drive\.google\.com\/file\/d\/(?P<file_id>[^\/\?]+)(?:\/.*)?(?:\?.*)?$', flags=re.IGNORECASE)
]


class Drive:
    ENDPOINT = 'https://www.googleapis.com/drive/v3'
    PAGE_LIMIT = 100
    def __init__(self, access_token, refresh_token_cb):
        self.access_token = access_token
        self.refresh_token_cb = refresh_token_cb
        self.http = requests.Session()
        self.http.headers['Authorization'] = f'Bearer {access_token}'
    
    def _get(self, url, *args, **kwargs):
        resp = self.http.get(url, *args, **kwargs)
        
        if resp.status_code == 401:
            self.access_token = self.refresh_token_cb()
            
            self.http.headers['Authorization'] = f'Bearer {self.access_token}'
            
            resp = self.http.get(url, *args, **kwargs)
        
        return resp
    
    def is_link(self, f):
        return f.mimeType == 'application/vnd.google-apps.shortcut'
    
    def is_dir(self, f):
        return f.mimeType == 'application/vnd.google-apps.folder'
    
    def folder(self, id):
        page_token = None
        
        while True:
            args = {
                'q': f"'{id}' in parents",
                'fields': 'nextPageToken, files(id, name, mimeType, createdTime, thumbnailLink, shortcutDetails)',
                'pageSize': self.PAGE_LIMIT
            }
            
            if page_token is not None:
                args['pageToken'] = page_token
            
            resp = self._get(f'{self.ENDPOINT}/files', params=args)
            files = hoordu.Dynamic.from_json(resp.text)
            
            for f in files.files:
                if self.is_link(f):
                    f.id = f.shortcutDetails.targetId
                    f.mimeType = f.shortcutDetails.targetMimeType
                
                yield f
            
            page_token = files.get('nextPageToken')
            
            if page_token is None:
                return
    
    def file(self, id):
        args = {
            'fields': 'id, name, mimeType, createdTime, thumbnailLink, shortcutDetails'
        }
        resp = self._get(f'{self.ENDPOINT}/files/{id}', params=args)
        return hoordu.Dynamic.from_json(resp.text)
    
    def file_url(self, file):
        return f'{self.ENDPOINT}/files/{file.id}?alt=media'


class GDrive(SimplePluginBase):
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
    def setup(cls, session, parameters=None):
        source = cls.get_source(session)
        
        # check if everything is ready to use
        config = hoordu.Dynamic.from_json(source.config)
        
        if not config.defined('client_id', 'client_secret'):
            # try to get the values from the parameters
            if parameters is not None:
                config.update(parameters)
                
                source.config = config.to_json()
                session.add(source)
        
        if not config.defined('client_id', 'client_secret'):
            # but if they're still None, the api can't be used
            return False, cls.config_form()
        
        elif not config.defined('access_token', 'refresh_token'):
            code = None
            if parameters is not None:
                code = parameters.get('code')
            
            oauth = OAuth({
                'auth_url': AUTH_URL,
                'token_url': TOKEN_URL,
                'callback_url': REDIRECT_URL,
                'scopes': SCOPES,
                'client_id': config.client_id,
                'client_secret': config.client_secret
            })
            
            if code is None:
                url = oauth.auth_url()
                
                oauth_form = Form('google authentication',
                    Label('please login to google via this url to get your authorization code:\n{}'.format(url)),
                    ('code', Input('code', [validators.required]))
                )
                
                return False, oauth_form
                
            else:
                response = oauth.get_access_token(code)
                
                config.access_token = response['access_token']
                config.refresh_token = response['refresh_token']
                source.config = config.to_json()
                session.add(source)
                
                return True, None
            
        else:
            # TODO check if everything is working
            
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
        for regexp in FILE_REGEXP:
            match = regexp.match(url)
            if match:
                return match.group('file_id')
        
        return None
    
    def __init__(self, session):
        super().__init__(session)
        
        self.http = urllib3.PoolManager()
        
        self.oauth = OAuth({
            'auth_url': AUTH_URL,
            'token_url': TOKEN_URL,
            'callback_url': REDIRECT_URL,
            'scopes': SCOPES,
            'client_id': self.config.client_id,
            'client_secret': self.config.client_secret
        })
        
        self.api = Drive(self.config.access_token, self._refresh_token)
    
    def _refresh_token(self):
        try:
            tokens = self.oauth.refresh_access_token(self.config.refresh_token)
        except OAuthError as e:
            msg = hoordu.Dynamic.from_json(str(e))
            if msg.error == 'invalid_grant':
                self.session.rollback()
                
                # refresh token expired or revoked
                self.config.pop('access_token')
                self.config.pop('refresh_token')
                self.source.config = self.config.to_json()
                self.session.add(self.source)
            
            raise
        
        access_token = tokens['access_token']
        
        # update access_token in the database
        self.config.access_token = access_token
        self.source.config = self.config.to_json()
        self.session.add(self.source)
        
        return access_token
    
    def _ordered_walk(self, node, base_path=''):
        for n in natsorted(self.api.folder(node.id), key=lambda x: (self.api.is_dir(x), x.name.lower())):
            path = base_path + n.name
            if not self.api.is_dir(n):
                yield path, n
            
            else:
                yield from self._ordered_walk(n, base_path=path + '/')
    
    def _download_file(self, file):
        url = self.api.file_url(file)
        headers = {'Authorization': f'Bearer {self.config.access_token}'}
        
        try:
            return self.session.download(url, headers=headers, suffix=file.name)[0]
            
        except HTTPError as e:
            if e.status == 401:
                self.api.access_token = self._refresh_token()
                headers = {'Authorization': f'Bearer {self.config.access_token}'}
                
                return self.session.download(url, headers=headers, suffix=file.name)[0]
    
    def download(self, id=None, remote_post=None, preview=False):
        if id is None and remote_post is None:
            raise ValueError('either id or remote_post must be passed')
        
        if remote_post is not None:
            id = remote_post.original_id
        
        node = self.api.file(id)
        
        original_id = node.id
        
        url = None
        if self.api.is_dir(node):
            url = FOLDER_FORMAT.format(file_id=node.id)
            
        else:
            url = FILE_FORMAT.format(file_id=node.id)
        
        create_time = dateutil.parser.parse(node.createdTime).astimezone(timezone.utc)
        
        if remote_post is None:
            remote_post = self.session.query(RemotePost).filter(RemotePost.source_id == self.source.id, RemotePost.original_id == original_id).one_or_none()
            if remote_post is None:
                remote_post = RemotePost(
                    source=self.source,
                    original_id=original_id,
                    title=node.name,
                    url=url,
                    type=PostType.set,
                    post_time=create_time
                )
                self.session.add(remote_post)
                
            else:
                self.log.info('post already exists: %s', remote_post.id)
        
        current_files = {file.metadata_: file for file in remote_post.files}
        
        if not self.api.is_dir(node):
            if len(remote_post.files) == 0:
                file = File(remote=remote_post, remote_order=1, filename=node.name)
                self.session.add(file)
                self.session.flush()
                
            else:
                file = remote_post.files[0]
            
            need_orig = not file.present and not preview
            
            if need_orig:
                self.log.info('downloading file for post: %s', remote_post.id)
                
                orig = self._download_file(node)
                
                self.session.import_file(file, orig=orig, move=True)
            
            return remote_post
        
        else:
            for (path, n), order in zip(self._ordered_walk(node), itertools.count(1)):
                id = n.id
                file = current_files.get(id)
                
                if file is None:
                    file = File(remote=remote_post, remote_order=order, filename=path, metadata_=id)
                    self.session.add(file)
                    self.session.flush()
                    self.log.info('found new file for post %s, file order: %s', remote_post.id, order)
                    
                else:
                    file.filename = path
                    file.remote_order = order
                    self.session.add(file)
                
                need_orig = not file.present and not preview
                
                if need_orig:
                    self.log.info('downloading file for post: %s', remote_post.id)
                    
                    orig = self._download_file(n)
                    
                    self.session.import_file(file, orig=orig, move=True)
            
            return remote_post

Plugin = GDrive


