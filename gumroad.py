import asyncio
import re
import itertools
from datetime import datetime, timezone
import dateutil.parser
import aiohttp
from bs4 import BeautifulSoup

import hoordu
from hoordu.models import *
from hoordu.plugins import *
from hoordu.forms import *
from hoordu.oauth.client import *
from hoordu.plugins.helpers import parse_href

PRODUCT_FORMAT = 'https://{account_code}.gumroad.com/l/{product_code}'
PRODUCT_REGEXP = [
    re.compile('^https?:\/\/(?P<account_code>[^\.]+)\.gumroad\.com\/l\/(?P<product_code>[^\/\?]+)(?:\?.*)?$', flags=re.IGNORECASE),
]


class Gumroad(SimplePlugin):
    name = 'gumroad'
    version = 1
    
    @classmethod
    def config_form(cls):
        return Form('{} config'.format(cls.name),
            ('gumroad_guid', Input('_gumroad_guid cookie', [validators.required])),
            ('gumroad_app_session', Input('_gumroad_app_session cookie', [validators.required])),
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
        
        if not config.contains('gumroad_app_session', 'gumroad_guid'):
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
        # TODO what is the id exactly???
        for regexp in PRODUCT_REGEXP:
            match = regexp.match(url)
            if match:
                return match.group('account_code') + '.' + match.group('product_code')
        
        return None
    
    
    @contextlib.asynccontextmanager
    async def context(self):
        async with super().context():
            self.cookies = {
                '_gumroad_guid': self.config.gumroad_guid,
                '_gumroad_app_session': self.config.gumroad_app_session,
            }
            
            async with aiohttp.ClientSession(cookies=self.cookies) as http:
                self.http: aiohttp.ClientSession = http
                yield self
    
    async def _download_file(self, url):
        path, resp = await self.session.download(url)
        return path
    
    async def download(self, id=None, remote_post=None, preview=False):
        if id is None and remote_post is None:
            raise ValueError('either id or remote_post must be passed')
        
        if remote_post is not None:
            id = remote_post.original_id
            account_id, product_code = id.split('/')
            metadata = hoordu.Dynamic.from_json(remote_post.metadata_)
            account_code = metadata.creator
            
        else:
            account_id = None
            account_code, product_code = id.split('.')
        
        main_url = PRODUCT_FORMAT.format(account_code=account_code, product_code=product_code)
        
        async with self.http.get(main_url) as response:
            response.raise_for_status()
            doc = BeautifulSoup(await response.text(), 'html.parser')
        
        post_json = doc.select('script[data-component-name="ProductPage"]')[0].text
        post = hoordu.Dynamic.from_json(post_json)
        
        account_id = post.product.seller.id
        original_id = f'{account_id}/{product_code}'
        
        # there's no time in gumroad lmao
        #create_time = dateutil.parser.parse(post.).astimezone(timezone.utc)
        
        if remote_post is None:
            remote_post = await self._get_post(original_id)
        
        metadata = hoordu.Dynamic()
        metadata.creator = account_code
        
        remote_post.title = post.product.name
        remote_post.url = main_url
        remote_post.type = PostType.set
        remote_post.metadata_ = metadata.to_json()
        
        self.log.info(f'downloading post: {remote_post.original_id}')
        self.log.info(f'local id: {remote_post.id}')
        
        remote_post.title = post.product.name
        
        comment_html = BeautifulSoup(post.product.description_html, 'html.parser')
        
        urls = []
        for a in comment_html.select('a'):
            url = parse_href(main_url, a['href'])
            urls.append(url)
            
            a.replace_with(url)
        
        for br in comment_html.find_all('br'):
            br.replace_with('\n')
        
        for para in comment_html.find_all('p'):
            para.replace_with(para.text + '\n')
        
        remote_post.comment = comment_html.text
        
        self.session.add(remote_post)
        
        user_tag = await self._get_tag(TagCategory.artist, account_id)
        await remote_post.add_tag(user_tag)
        
        if user_tag.update_metadata('name', post.product.seller.name):
            self.session.add(user_tag)
        
        for url in urls:
            await remote_post.add_related_url(url)
        
        if post.purchase is not None:
            # download files
            content_url = parse_href(main_url, post.purchase.content_url)
            async with self.http.get(content_url) as response:
                response.raise_for_status()
                doc = BeautifulSoup(await response.text(), 'html.parser')
            
            content_json = doc.select('script[data-component-name="ContentContainer"]')[0].text
            content = hoordu.Dynamic.from_json(content_json)
            
            files = await remote_post.fetch(RemotePost.files)
            current_files = {file.metadata_: file for file in files}
            
            for item, order in zip(content.content_items, itertools.count(1)):
                if item.type == 'file':
                    # item.id -> metadata
                    id = item.id
                    file = current_files.get(id)
                    filename = f'{item.file_name}.{item.extension.lower()}'
                    
                    if file is None:
                        file = File(remote=remote_post, remote_order=order, filename=filename, metadata_=id)
                        self.session.add(file)
                        await self.session.flush()
                        
                    else:
                        file.remote_order = order
                        file.filename = filename
                        self.session.add(file)
                    
                    need_orig = not file.present and not preview
                    if need_orig:
                        self.log.info(f'downloading file: {file.remote_order}')
                        
                        download_url = parse_href(content_url, item.download_url)
                        orig = await self._download_file(download_url)
                        
                        await self.session.import_file(file, orig=orig, move=True)
        
        return remote_post

Plugin = Gumroad


