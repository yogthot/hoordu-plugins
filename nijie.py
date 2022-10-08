#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import re
from datetime import datetime, timedelta, timezone
import dateutil.parser
from tempfile import mkstemp
import shutil
from urllib import parse as urlparse
import itertools
import functools
from xml.sax.saxutils import unescape

import requests
from bs4 import BeautifulSoup

import hoordu
from hoordu.models import *
from hoordu.plugins import *
from hoordu.forms import *


POST_FORMAT = 'https://nijie.info/view.php?id={post_id}'
POST_URL = ['nijie.info/view.php']
USER_INFO_URL = 'https://nijie.info/members.php'
USER_ILLUST_URL = 'https://nijie.info/members_illust.php'
USER_URL = [
    'nijie.info/members.php',
    'nijie.info/members_illust.php',
    'nijie.info/members_dojin.php',
]

class UserIterator(IteratorBase):
    def __init__(self, plugin, subscription=None, options=None):
        super().__init__(plugin, subscription=subscription, options=options)
        
        self.http = plugin.http
        
        self.first_id = None
        self.state.head_id = self.state.get('head_id')
        self.state.tail_id = self.state.get('tail_id')
        self.state.tail_page = self.state.get('tail_page', 1)
    
    def reconfigure(self, direction=FetchDirection.newer, num_posts=None):
        if direction == FetchDirection.newer:
            if self.state.tail_id is None:
                direction = FetchDirection.older
            else:
                num_posts = None
        
        super().reconfigure(direction=direction, num_posts=num_posts)
    
    def _get_page(self, page_id=None):
        # https://nijie.info/members_illust.php?p={page_id (1 indexed)}&id={user_id}
        if page_id is None: page_id = 1
        
        params = {
            'p': page_id,
            'id': self.options.user_id,
        }
        response = self.http.get(USER_ILLUST_URL, params=params)
        response.raise_for_status()
        html = BeautifulSoup(response.text, 'html.parser')
        
        post_urls = [e['href'] for e in html.select('#members_dlsite_left .picture a')]
        return [int(urlparse.parse_qs(urlparse.urlparse(url).query)['id'][0]) for url in post_urls]
    
    def _iterator(self):
        page_id = self.state.tail_page if self.direction == FetchDirection.older else 1
        
        first_iteration = True
        while True:
            post_ids = self._get_page(page_id)
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
                
                yield self.plugin._to_remote_post(str(post_id), preview=self.subscription is None)
                
                if self.direction == FetchDirection.older:
                    self.state.tail_id = post_id
                
                if self.num_posts is not None:
                    self.num_posts -= 1
                    if self.num_posts <= 0:
                        return
                
                first_iteration = False
            
            page_id += 1
    
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
    def setup(cls, session, parameters=None):
        plugin = cls.get_plugin(session)
        
        # check if everything is ready to use
        config = hoordu.Dynamic.from_json(plugin.config)
        
        if not config.contains('NIJIEIJIEID', 'nijie_tok'):
            # try to get the values from the parameters
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
    def update(cls, session):
        plugin = cls.get_plugin(session)
        
        if plugin.version < cls.version:
            # update anything if needed
            
            # if anything was updated, then the db entry should be updated as well
            plugin.version = cls.version
            session.add(plugin)
    
    @classmethod
    def parse_url(cls, url):
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
    
    def __init__(self, session):
        super().__init__(session)
        
        self.http = requests.Session()
        
        self._headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:80.0) Gecko/20100101 Firefox/100.0'
        }
        self.http.headers.update(self._headers)
        
        self.cookies = {
            'NIJIEIJIEID': self.config.NIJIEIJIEID,
            'nijie_tok': self.config.nijie_tok,
        }
        
        for name, val in self.cookies.items():
            cookie = requests.cookies.create_cookie(name=name, value=val)
            self.http.cookies.set_cookie(cookie)
    
    def _download_file(self, url):
        path, resp = self.session.download(url, cookies=self.cookies)
        return path
    
    def _parse_href(self, page_url, href):
        if re.match('^https?:\/\/\S+$', href):
            return href
        
        if href.startswith('//'):
            base_url = re.match('^[^:]+:', page_url).group(0)
            return base_url + href
            
        elif href.startswith('/'):
            base_url = re.match('^[^:]+:\/\/[^\/]+', page_url).group(0)
            return base_url + href
        
        else:
            base_url = re.match('^.*/', page_url).group(0)
            return base_url + href
        
    
    def _to_remote_post(self, id, remote_post=None, preview=False):
        url = POST_FORMAT.format(post_id=id)
        
        response = self.http.get(url)
        response.raise_for_status()
        post = BeautifulSoup(response.text, 'html.parser')
        
        title = post.select('.illust_title')[0].text
        
        imgs = post.select("#gallery img.mozamoza")
        user_id = imgs[0]['user_id']
        
        user_name = list(post.select("#pro .name")[0].children)[2]
        
        timestamp = post.select("#view-honbun span")[0].text.split('：', 1)[-1]
        post_time = dateutil.parser.parse(timestamp).astimezone(timezone.utc)
        
        if remote_post is None:
            remote_post = self._get_post(id)
        
        if remote_post is None:
            remote_post = RemotePost(
                source=self.source,
                original_id=id,
                url=url,
                title=title,
                type=PostType.set,
                post_time=post_time
            )
            
            self.session.add(remote_post)
            self.session.flush()
        
        self.log.info(f'downloading post: {remote_post.original_id}')
        self.log.info(f'local id: {remote_post.id}')
        
        
        comment_html = post.select('#illust_text')[0]
        
        urls = []
        page_url = POST_FORMAT.format(post_id=id)
        for a in comment_html.select('a'):
            url = self._parse_href(page_url, a['href'])
            urls.append(url)
            
            a.replace_with(url)
        
        for br in comment_html.find_all('br'):
            br.replace_with('\n')
        
        for para in comment_html.find_all('p'):
            para.replace_with(para.text + '\n')
        
        remote_post.comment = comment_html.text
        
        user_tag = self._get_tag(TagCategory.artist, user_id)
        remote_post.add_tag(user_tag)
        
        if user_tag.update_metadata('name', user_name):
            self.session.add(user_tag)
        
        tags = post.select('#view-tag li.tag a')
        for tag in tags:
            remote_tag = self._get_tag(TagCategory.general, tag.text)
            remote_post.add_tag(remote_tag)
        
        for url in urls:
            remote_post.add_related_url(url)
        
        # files
        available = set(range(len(imgs)))
        present = set(file.remote_order for file in remote_post.files)
        
        for order in available - present:
            file = File(remote=remote_post, remote_order=order)
            self.session.add(file)
            self.session.flush()
        
        for file in remote_post.files:
            img = imgs[file.remote_order]
            
            orig_url = 'https:' + img['src'].replace('__rs_l120x120/', '')
            thumb_url = orig_url.replace('/nijie/', '/__rs_l120x120/nijie/')
            
            need_orig = not file.present and not preview
            need_thumb = not file.thumb_present
            
            if need_thumb or need_orig:
                self.log.info(f'downloading file: {file.remote_order}')
                
                orig = self._download_file(orig_url) if need_orig else None
                thumb = self._download_file(thumb_url) if need_thumb else None
                
                self.session.import_file(file, orig=orig, thumb=thumb, move=True)
        
        return remote_post
    
    def download(self, id=None, remote_post=None, preview=False):
        if id is None and remote_post is None:
            raise ValueError('either id or remote_post must be passed')
        
        if remote_post is not None:
            id = remote_post.original_id
        
        return self._to_remote_post(id, remote_post=remote_post, preview=preview)
    
    def search_form(self):
        return Form('{} search'.format(self.name),
            ('user_id', Input('user id', [validators.required()]))
        )
    
    def get_search_details(self, options):
        response = self.http.get(USER_INFO_URL, params={'id': options.user_id})
        response.raise_for_status()
        html = BeautifulSoup(response.text, 'html.parser')
        
        user_name = list(html.select("#pro .name")[0].children)[2]
        thumbnail_url = html.select("#pro img")[0]['src'].replace("__rs_cs150x150/", "")
        
        
        desc_html = html.select('#prof-l')[0]
        
        urls = []
        page_url = POST_FORMAT.format(post_id=id)
        for a in desc_html.select('a'):
            url = a.text
            urls.append(url)
            
            a.replace_with(url)
        
        for dt in desc_html.find_all('dt'):
            dt.replace_with(dt.text + ' | ')
        
        for dd in desc_html.find_all('dd'):
            dd.replace_with(dd.text + '\n')
        
        return SearchDetails(
            hint=user_name,
            title=user_name,
            description=desc_html.text,
            thumbnail_url=thumbnail_url,
            related_urls=urls
        )
    
    def subscription_repr(self, options):
        return 'user:{}'.format(options.user_id)
    
Plugin = Pixiv


