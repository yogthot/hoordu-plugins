#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import re
from datetime import datetime, timedelta, timezone
import dateutil.parser
from urllib import parse as urlparse

import aiohttp
from bs4 import BeautifulSoup

import hoordu
from hoordu.models import *
from hoordu.plugins import *
from hoordu.forms import *
from hoordu.plugins.helpers import parse_href

POST_FORMAT = 'https://nijie.info/view.php?id={post_id}'
POST_URL = ['nijie.info/view.php']
USER_INFO_URL = 'https://nijie.info/members.php'
USER_ILLUST_URL = 'https://nijie.info/members_illust.php'
USER_URL = [
    'nijie.info/members.php',
    'nijie.info/members_illust.php',
    'nijie.info/members_dojin.php',
]

class UserIterator(IteratorBase['Nijie']):
    def __init__(self, plugin, subscription=None, options=None):
        super().__init__(plugin, subscription=subscription, options=options)
        
        self.http: aiohttp.ClientSession = plugin.http
        
        self.first_id = None
        self.state.head_id = self.state.get('head_id')
        self.state.tail_id = self.state.get('tail_id')
        self.state.tail_page = self.state.get('tail_page', 1)
    
    def __repr__(self):
        return 'user:{}'.format(self.options.user_id)
    
    def reconfigure(self, direction=FetchDirection.newer, num_posts=None):
        if direction == FetchDirection.newer:
            if self.state.tail_id is None:
                direction = FetchDirection.older
            else:
                num_posts = None
        
        super().reconfigure(direction=direction, num_posts=num_posts)
    
    async def _get_page(self, page_id=None):
        # https://nijie.info/members_illust.php?p={page_id (1 indexed)}&id={user_id}
        if page_id is None: page_id = 1
        
        params = {
            'p': page_id,
            'id': self.options.user_id,
        }
        async with self.http.get(USER_ILLUST_URL, params=params) as response:
            response.raise_for_status()
            html = BeautifulSoup(await response.text(), 'html.parser')
        
        post_urls = [e['href'] for e in html.select('#members_dlsite_left .picture a')]
        return [int(urlparse.parse_qs(urlparse.urlparse(url).query)['id'][0]) for url in post_urls]
    
    async def _iterator(self):
        page_id = self.state.tail_page if self.direction == FetchDirection.older else 1
        
        first_iteration = True
        while True:
            post_ids = await self._get_page(page_id)
            if len(post_ids) == 0:
                # empty page, stopping
                return
            
            if self.direction == FetchDirection.older:
                self.state.tail_page = page_id
            
            for post_id in post_ids:
                if self.direction == FetchDirection.newer and post_id <= self.state.head_id:
                    return
                
                if self.direction == FetchDirection.older and self.state.tail_id is not None and post_id >= self.state.tail_id:
                    continue
                
                if first_iteration and (self.state.head_id is None or self.direction == FetchDirection.newer):
                    self.first_id = post_id
                
                yield await self.plugin._to_remote_post(str(post_id), preview=self.subscription is None)
                
                if self.direction == FetchDirection.older:
                    self.state.tail_id = post_id
                
                if self.num_posts is not None:
                    self.num_posts -= 1
                    if self.num_posts <= 0:
                        return
                
                first_iteration = False
            
            page_id += 1
    
    async def generator(self):
        async for post in self._iterator():
            yield post
            
            if self.subscription is not None:
                await self.subscription.add_post(post)
            
            await self.session.commit()
        
        if self.first_id is not None:
            self.state.head_id = self.first_id
            self.first_id = None
        
        if self.subscription is not None:
            self.subscription.state = self.state.to_json()
            self.session.add(self.subscription)
        
        await self.session.commit()

class Nijie(SimplePlugin):
    name = 'nijie'
    version = 1
    iterator = UserIterator
    
    @classmethod
    def config_form(cls):
        return Form('{} config'.format(cls.name),
            ('NIJIEIJIEID', Input('NIJIEIJIEID cookie', [validators.required])),
            ('nijie_tok', Input('nijie_tok cookie', [validators.required])),
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
        
        if not config.contains('NIJIEIJIEID', 'nijie_tok'):
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
        
        p = urlparse.urlparse(url)
        part = p.netloc + p.path
        query = urlparse.parse_qs(p.query)
        
        if part in POST_URL:
            return query['id'][0]
        
        if part in USER_URL:
            return hoordu.Dynamic({
                'user_id': query['id'][0]
            })
        
        return None
    
    @contextlib.asynccontextmanager
    async def context(self):
        async with super().context():
            self._headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:80.0) Gecko/20100101 Firefox/100.0'
            }
            self.cookies = {
                'NIJIEIJIEID': self.config.NIJIEIJIEID,
                'nijie_tok': self.config.nijie_tok,
            }
            
            async with aiohttp.ClientSession(headers=self._headers, cookies=self.cookies) as http:
                self.http: aiohttp.ClientSession = http
                yield self
    
    async def _download_file(self, url):
        path, resp = await self.session.download(url, cookies=self.cookies)
        return path
    
    async def _to_remote_post(self, id, remote_post=None, preview=False):
        url = POST_FORMAT.format(post_id=id)
        
        async with self.http.get(url) as response:
            response.raise_for_status()
            post = BeautifulSoup(await response.text(), 'html.parser')
        
        # if there is no title, chances are the cookie doesn't work
        # TODO need a way to detect if the cookie works or not, no error code is returned
        title = post.select('.illust_title')[0].text
        
        files = post.select("#gallery .mozamoza")
        user_id = files[0]['user_id']
        
        user_name = list(post.select("#pro .name")[0].children)[2]
        
        timestamp = post.select("#view-honbun span")[0].text.split('：', 1)[-1]
        post_time = dateutil.parser.parse(timestamp).astimezone(timezone.utc).replace(tzinfo=None)
        
        if remote_post is None:
            remote_post = await self._get_post(id)
        
        remote_post.url = url
        remote_post.title = title
        remote_post.type = PostType.set
        remote_post.post_time = post_time
        
        self.log.info(f'downloading post: {remote_post.original_id}')
        self.log.info(f'local id: {remote_post.id}')
        
        
        comment_html = post.select('#illust_text')[0]
        
        urls = []
        page_url = POST_FORMAT.format(post_id=id)
        for a in comment_html.select('a'):
            url = parse_href(page_url, a['href'])
            urls.append(url)
            
            a.replace_with(url)
        
        for br in comment_html.find_all('br'):
            br.replace_with('\n')
        
        for para in comment_html.find_all('p'):
            para.replace_with(para.text + '\n')
        
        remote_post.comment = comment_html.text
        self.session.add(remote_post)
        
        
        user_tag = await self._get_tag(TagCategory.artist, user_id)
        await remote_post.add_tag(user_tag)
        
        if user_tag.update_metadata('name', user_name):
            self.session.add(user_tag)
        
        tags = post.select('#view-tag li.tag a')
        for tag in tags:
            remote_tag = await self._get_tag(TagCategory.general, tag.text)
            await remote_post.add_tag(remote_tag)
        
        for url in urls:
            await remote_post.add_related_url(url)
        
        # files
        available = set(range(len(files)))
        present = set(file.remote_order for file in await remote_post.fetch(RemotePost.files))
        
        for order in available - present:
            file = File(remote=remote_post, remote_order=order)
            self.session.add(file)
            await self.session.flush()
        
        for file in await remote_post.fetch(RemotePost.files):
            f = files[file.remote_order]
            
            orig_url = parse_href(page_url, f['src'].replace('__rs_l120x120/', ''))
            thumb_url = orig_url.replace('/nijie/', '/__rs_l120x120/nijie/')
            
            need_orig = not file.present and not preview
            need_thumb = not file.thumb_present
            
            if need_thumb or need_orig:
                self.log.info(f'downloading file: {file.remote_order}')
                
                orig = await self._download_file(orig_url) if need_orig else None
                thumb = await self._download_file(thumb_url) if need_thumb else None
                
                await self.session.import_file(file, orig=orig, thumb=thumb, move=True)
        
        return remote_post
    
    async def download(self, id=None, remote_post=None, preview=False):
        if id is None and remote_post is None:
            raise ValueError('either id or remote_post must be passed')
        
        if remote_post is not None:
            id = remote_post.original_id
        
        return await self._to_remote_post(id, remote_post=remote_post, preview=preview)
    
    def search_form(self):
        return Form('{} search'.format(self.name),
            ('user_id', Input('user id', [validators.required()]))
        )
    
    async def get_search_details(self, options):
        async with self.http.get(USER_INFO_URL, params={'id': options.user_id}) as response:
            response.raise_for_status()
            html = BeautifulSoup(await response.text(), 'html.parser')
        
        user_name = list(html.select("#pro .name")[0].children)[2]
        thumbnail_url = html.select("#pro img")[0]['src'].replace("__rs_cs150x150/", "")
        
        
        desc_html = html.select('#prof-l')[0]
        
        urls = set()
        page_url = POST_FORMAT.format(post_id=id)
        for a in desc_html.select('a'):
            url = a.text
            urls.add(url)
            
            a.replace_with(url)
        
        for dt in desc_html.find_all('dt'):
            dt.replace_with(dt.text + ' | ')
        
        for dd in desc_html.find_all('dd'):
            dd.replace_with(dd.text + '\n')
        
        return SearchDetails(
            hint=user_name.text,
            title=user_name.text,
            description=desc_html.text,
            thumbnail_url=thumbnail_url,
            related_urls=urls
        )
    
Plugin = Nijie


