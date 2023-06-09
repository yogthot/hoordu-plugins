#!/usr/bin/env python3

import re
from datetime import datetime, timedelta, timezone
import dateutil.parser
import itertools

import aiohttp
from bs4 import BeautifulSoup

import hoordu
from hoordu.models import *
from hoordu.plugins import *
from hoordu.forms import *

CREATOR_ID_GET_URL = 'https://www.pixiv.net/fanbox/creator/{pixiv_id}'
CREATOR_GET_URL = 'https://api.fanbox.cc/creator.get?creatorId={creator}'
CREATOR_URL_REGEXP = re.compile('https?:\/\/(?P<creator>[^\.]+)\.fanbox\.cc\/', flags=re.IGNORECASE)
PIXIV_URL = 'https://www.pixiv.net/en/users/{pixiv_id}'

POST_FORMAT = 'https://fanbox.cc/@{creator}/posts/{post_id}'
POST_REGEXP = [
    re.compile('^https?:\/\/(?P<creator>[^\.]+)\.fanbox\.cc\/posts\/(?P<post_id>\d+)(?:\?.*)?(?:#.*)?$', flags=re.IGNORECASE),
    re.compile('^https?:\/\/(?:www\.)?fanbox\.cc\/@(?P<creator>[^\/]*)\/posts\/(?P<post_id>\d+)(?:\?.*)?(?:#.*)?$', flags=re.IGNORECASE)
]
CREATOR_REGEXP = [
    re.compile('^https?:\/\/(?:www\.)?fanbox\.cc\/@(?P<creator>[^\/]+)(?:\/.*)?(?:\?.*)?(?:#.*)?$', flags=re.IGNORECASE),
    re.compile('^https?:\/\/(?P<creator>[^\.]+)\.fanbox\.cc(?:\/.*)?(?:\?.*)?(?:#.*)?$', flags=re.IGNORECASE),
]

POST_GET_URL = 'https://api.fanbox.cc/post.info?postId={post_id}'
POST_EMBED_INFO_URL = 'https://api.fanbox.cc/post.get?postId={related_post_id}'
CREATOR_POSTS_URL = 'https://api.fanbox.cc/post.listCreator'
PAGE_LIMIT = 10

class CreatorIterator(IteratorBase['Fanbox']):
    def __init__(self, fanbox, subscription=None, options=None):
        super().__init__(fanbox, subscription=subscription, options=options)
        
        self.http: aiohttp.ClientSession = fanbox.http
        
        self.options.pixiv_id = self.options.get('pixiv_id')
        
        self.first_id = None
        self.state.head_id = self.state.get('head_id')
        self.state.tail_id = self.state.get('tail_id')
        self.state.tail_datetime = self.state.get('tail_datetime')
        
        self.downloaded = set()
    
    def __repr__(self):
        return 'posts:{}'.format(self.options.pixiv_id)
    
    async def init(self):
        update = False
        
        if self.options.pixiv_id is not None:
            creator = await self.plugin._get_creator_id(self.options.pixiv_id)
            
            if creator and self.options.creator != creator:
                self.options.creator = self.options.creator = creator
                update = True
            
        else:
            async with self.http.get(CREATOR_GET_URL.format(creator=self.options.creator)) as response:
                response.raise_for_status()
                creator = hoordu.Dynamic.from_json(await response.text()).body
            
            self.options.pixiv_id = creator.user.userId
            update = True
        
        if update and self.subscription is not None:
            self.subscription.repr = repr(self)
            self.subscription.options = self.options.to_json()
            self.session.add(self.subscription)
    
    def reconfigure(self, direction=FetchDirection.newer, num_posts=None):
        if direction == FetchDirection.newer:
            if self.state.tail_id is None:
                direction = FetchDirection.older
            else:
                num_posts = None
        
        super().reconfigure(direction=direction, num_posts=num_posts)
        
        self.total = 0
        self.downloaded = set()
    
    async def _post_iterator(self):
        head = (self.direction == FetchDirection.newer)
        
        min_id = int(self.state.head_id) if head and self.state.head_id is not None else None
        max_id = self.state.tail_id if not head else None
        max_datetime = self.state.tail_datetime if not head else None
        
        first_iteration = True
        while True:
            page_size = PAGE_LIMIT
            if self.num_posts is not None:
                page_size = min(self.num_posts - self.total, PAGE_LIMIT)
            
            params = {
                'creatorId': self.options.creator,
                'limit': page_size
            }
            
            if max_id is not None:
                params['maxId'] = int(max_id) - 1
                # very big assumption that no posts have the time timestamp
                # fanbox would break if that happened as well
                d = dateutil.parser.parse(max_datetime).replace(tzinfo=None)
                params['maxPublishedDatetime'] = (d - timedelta(seconds=1)).strftime('%Y-%m-%d %H:%M:%S')
            
            async with self.http.get(CREATOR_POSTS_URL, params=params) as response:
                response.raise_for_status()
                body = hoordu.Dynamic.from_json(await response.text()).body
                posts = body['items']
            
            if len(posts) == 0:
                return
            
            if first_iteration and (self.state.head_id is None or self.direction == FetchDirection.newer):
                self.first_id = posts[0].id
            
            for post in posts:
                id = int(post.id)
                if min_id is not None and id <= min_id:
                    return
                
                yield post
                
                max_id = id - 1
                max_datetime = post.publishedDatetime
                
                if self.direction == FetchDirection.older:
                    self.state.tail_id = post.id
                    self.state.tail_datetime = post.publishedDatetime
                
                self.total += 1
                if self.num_posts is not None and self.total >= self.num_posts:
                    return
            
            if body.nextUrl is None:
                return
            
            first_iteration = False
    
    async def generator(self):
        async for post in self._post_iterator():
            if post.id in self.downloaded:
                continue
            
            async with self.http.get(POST_GET_URL.format(post_id=post.id)) as response:
                response.raise_for_status()
                post_body = hoordu.Dynamic.from_json(await response.text()).body
            
            remote_post = await self.plugin._to_remote_post(post_body, preview=self.subscription is None)
            yield remote_post
            
            self.downloaded.add(post.id)
            
            if self.subscription is not None:
                await self.subscription.add_post(remote_post)
            
            await self.session.commit()
        
        if self.first_id is not None:
            self.state.head_id = self.first_id
            self.first_id = None
        
        if self.subscription is not None:
            self.subscription.state = self.state.to_json()
            self.session.add(self.subscription)
        
        await self.session.commit()

class Fanbox(SimplePlugin):
    name = 'fanbox'
    version = 1
    iterator = CreatorIterator
    
    @classmethod
    def config_form(cls):
        return Form('{} config'.format(cls.name),
            ('FANBOXSESSID', Input('FANBOXSESSID cookie', [validators.required]))
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
        
        if not config.contains('FANBOXSESSID'):
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
        
        for regexp in CREATOR_REGEXP:
            match = regexp.match(url)
            if match:
                return hoordu.Dynamic({
                    'creator': match.group('creator')
                })
        
        return None
    
    @contextlib.asynccontextmanager
    async def context(self):
        async with super().context():
            self._headers = {
                'Origin': 'https://www.fanbox.cc',
                'Referer': 'https://www.fanbox.cc/',
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:80.0) Gecko/20100101 Firefox/82.0'
            }
            self._cookies = {
                'FANBOXSESSID': self.config.FANBOXSESSID
            }
            
            async with aiohttp.ClientSession(headers=self._headers, cookies=self._cookies) as http:
                self.http: aiohttp.ClientSession = http
                yield self
    
    async def _get_creator_id(self, pixiv_id):
        async with self.http.get(CREATOR_ID_GET_URL.format(pixiv_id=pixiv_id), allow_redirects=False) as response:
            creator_url = response.headers['Location']
        
        match = CREATOR_URL_REGEXP.match(creator_url)
        return match.group('creator')
    
    async def _download_file(self, url):
        path, resp = await self.session.download(url, headers=self._headers, cookies=self._cookies)
        return path
    
    async def _to_remote_post(self, post, remote_post=None, preview=False):
        main_id = post.id
        creator_id = post.user.userId
        creator_slug = post.creatorId
        creator_name = post.user.name
        # possible timezone issues?
        post_time = dateutil.parser.parse(post.publishedDatetime).astimezone(timezone.utc).replace(tzinfo=None)
        
        if remote_post is None:
            remote_post = await self._get_post(main_id)
        
        remote_post.url = POST_FORMAT.format(creator=creator_slug, post_id=main_id)
        remote_post.title = post.title
        remote_post.type = PostType.collection
        remote_post.post_time = post_time
        
        metadata = hoordu.Dynamic()
        if post.feeRequired != 0:
            metadata.price = post.feeRequired
        
        remote_post.metadata_ = metadata.to_json()
        self.session.add(remote_post)
        
        self.log.info(f'downloading post: {remote_post.original_id}')
        self.log.info(f'local id: {remote_post.id}')
        
        if post.isLiked is True:
            remote_post.favorite = True
        
        # creators are identified by their pixiv id because their name and creatorId can change
        creator_tag = await self._get_tag(TagCategory.artist, creator_id)
        await remote_post.add_tag(creator_tag)
        
        if any((creator_tag.update_metadata('name', creator_name),
                creator_tag.update_metadata('slug', creator_slug))):
            self.session.add(creator_tag)
        
        for tag in post.tags:
            remote_tag = await self._get_tag(TagCategory.general, tag)
            await remote_post.add_tag(remote_tag)
        
        if post.hasAdultContent is True:
            nsfw_tag = await self._get_tag(TagCategory.meta, 'nsfw')
            await remote_post.add_tag(nsfw_tag)
        
        current_files = {file.metadata_: file for file in await remote_post.fetch(RemotePost.files)}
        current_urls = [r.url for r in  await remote_post.fetch(RemotePost.related)]
        
        if not post.isRestricted:
            if post.type == 'image':
                for image, order in zip(post.body.images, itertools.count(1)):
                    id = 'i-{}'.format(image.id)
                    file = current_files.get(id)
                    
                    if file is None:
                        file = File(remote=remote_post, remote_order=order, metadata_=id)
                        self.session.add(file)
                        await self.session.flush()
                        
                    else:
                        file.remote_order = order
                        self.session.add(file)
                    
                    need_orig = not file.present and not preview
                    need_thumb = not file.thumb_present
                    
                    if need_thumb or need_orig:
                        self.log.info(f'downloading file: {file.remote_order}')
                        
                        orig = await self._download_file(image.originalUrl) if need_orig else None
                        thumb = await self._download_file(image.thumbnailUrl) if need_thumb else None
                        
                        await self.session.import_file(file, orig=orig, thumb=thumb, move=True)
                
                remote_post.comment = post.body.text
                self.session.add(remote_post)
                
            elif post.type == 'file':
                for rfile, order in zip(post.body.files, itertools.count(1)):
                    id = 'f-{}'.format(rfile.id)
                    file = current_files.get(id)
                    
                    if file is None:
                        filename = '{0.name}.{0.extension}'.format(rfile)
                        file = File(remote=remote_post, remote_order=order, filename=filename, metadata_=id)
                        self.session.add(file)
                        await self.session.flush()
                        
                    else:
                        file.remote_order = order
                        self.session.add(file)
                    
                    need_orig = not file.present and not preview
                    
                    if need_orig:
                        self.log.info(f'downloading file: {file.remote_order}')
                        
                        orig = await self._download_file(rfile.url)
                        
                        await self.session.import_file(file, orig=orig, move=True)
                
                remote_post.comment = post.body.text
                self.session.add(remote_post)
                
            elif post.type == 'article':
                imagemap = post.body.get('imageMap')
                filemap = post.body.get('fileMap')
                embedmap = post.body.get('embedMap')
                urlembedmap = post.body.get('urlEmbedMap')
                
                order = 1
                
                blog = []
                for block in post.body.blocks:
                    if block.type in ('p', 'header'):
                        links = block.get('links')
                        if links is not None:
                            for link in links:
                                url = link.url
                                if url not in current_urls:
                                     await remote_post.add_related_url(url)
                        
                        blog.append({
                            'type': 'text',
                            'content': block.text + '\n'
                        })
                        
                    elif block.type == 'image':
                        id = 'i-{}'.format(block.imageId)
                        file = current_files.get(id)
                        
                        if file is None:
                            file = File(remote=remote_post, remote_order=order, metadata_=id)
                            self.session.add(file)
                            await self.session.flush()
                            
                        else:
                            file.remote_order = order
                            self.session.add(file)
                        
                        orig_url = imagemap[block.imageId].originalUrl
                        thumb_url = imagemap[block.imageId].thumbnailUrl
                        
                        need_orig = not file.present and not preview
                        need_thumb = not file.thumb_present
                        
                        if need_thumb or need_orig:
                            self.log.info(f'downloading file: {file.remote_order}')
                            
                            orig = await self._download_file(orig_url) if need_orig else None
                            thumb = await self._download_file(thumb_url) if need_thumb else None
                            
                            await self.session.import_file(file, orig=orig, thumb=thumb, move=True)
                        
                        blog.append({
                            'type': 'file',
                            'metadata': id
                        })
                        
                        order += 1
                        
                    elif block.type == 'file':
                        id = 'f-{}'.format(block.fileId)
                        file = current_files.get(id)
                        
                        if file is None:
                            file = File(remote=remote_post, remote_order=order, metadata_=id)
                            self.session.add(file)
                            await self.session.flush()
                        
                        orig_url = filemap[block.fileId].url
                        thumb_url = post.coverImageUrl
                        
                        need_orig = not file.present and not preview
                        need_thumb = not file.thumb_present and thumb_url is not None
                        
                        if need_thumb or need_orig:
                            self.log.info(f'downloading file: {file.remote_order}')
                            
                            orig = await self._download_file(orig_url) if need_orig else None
                            thumb = await self._download_file(thumb_url) if need_thumb else None
                            
                            await self.session.import_file(file, orig=orig, thumb=thumb, move=True)
                        
                        blog.append({
                            'type': 'file',
                            'metadata': id
                        })
                        
                        order += 1
                        
                    elif block.type == 'embed':
                        embed = embedmap[block.embedId]
                        
                        url = None
                        if embed.serviceProvider == 'fanbox':
                            related_post_id = embed.contentId.split('/')[-1]
                            
                            async with self.http.get(POST_EMBED_INFO_URL.format(related_post_id=related_post_id)) as response:
                                response.raise_for_status()
                                related_post_body = hoordu.Dynamic.from_json(await response.text()).body
                            
                            url = POST_FORMAT.format(creator=related_post_body.creatorId, post_id=related_post_id)
                            
                        elif embed.serviceProvider == 'google_forms':
                            url = 'https://docs.google.com/forms/d/e/{}/viewform'.format(embed.contentId)
                            
                        elif embed.serviceProvider == 'twitter':
                            url = 'https://twitter.com/i/web/status/{}'.format(embed.contentId)
                            
                        else:
                            self.log.warning('unknown embed service provider: %s', str(embed.serviceProvider))
                        
                        if url:
                            if url not in current_urls:
                                await remote_post.add_related_url(url)
                            
                            blog.append({
                                'type': 'text',
                                'content': url + '\n'
                            })
                        
                    elif block.type == 'url_embed':
                        urlembed = urlembedmap[block.urlEmbedId]
                        
                        url = None
                        if urlembed.type == 'fanbox.post':
                            related_post_id = urlembed.postInfo.id
                            related_creator_id = urlembed.postInfo.creatorId
                            url = POST_FORMAT.format(creator=related_creator_id, post_id=related_post_id)
                            
                        elif urlembed.type in ('html', 'html.card'):
                            embed_html = BeautifulSoup(urlembed.html, 'html.parser')
                            iframes = embed_html.select('iframe')
                            if len(iframes) >= 1:
                                url = iframes[0]['src']
                            else:
                                self.log.warning('no iframe found on html/card embed: %s', str(urlembed.html))
                            
                        else:
                            self.log.warning('unknown url_embed type: %s', str(urlembed.type))
                        
                        if url:
                            if url not in current_urls:
                                await remote_post.add_related_url(url)
                            
                            blog.append({
                                'type': 'text',
                                'content': url + '\n'
                            })
                        
                    else:
                        self.log.warning('unknown blog block: %s', str(block.type))
                
                remote_post.comment = hoordu.Dynamic({'comment': blog}).to_json()
                remote_post.type = PostType.blog
                self.session.add(remote_post)
                
            elif post.type == 'text':
                remote_post.comment = post.body.text
                remote_post.type = PostType.set
                self.session.add(remote_post)
                
            else:
                raise NotImplementedError('unknown post type: {}'.format(post.type))
            
        else:
            # restrited post (body is null)
            pass
            
        
        return remote_post
    
    async def download(self, id=None, remote_post=None, preview=False):
        if id is None and remote_post is None:
            raise ValueError('either id or remote_post must be passed')
        
        if remote_post is not None:
            id = remote_post.original_id
        
        async with self.http.get(POST_GET_URL.format(post_id=id)) as response:
            response.raise_for_status()
            post = hoordu.Dynamic.from_json(await response.text()).body
        
        self.log.debug('post json: %s', post)
        
        if post.isRestricted:
            self.log.warning('inaccessible post %s', id)
        
        return await self._to_remote_post(post, remote_post=remote_post, preview=preview)
    
    def search_form(self):
        return Form('{} search'.format(self.name),
            ('creator', Input('creator', [validators.required()]))
        )
    
    async def get_search_details(self, options):
        pixiv_id = options.get('pixiv_id')
        
        creator_id = self._get_creator_id(pixiv_id) if pixiv_id else options.creator 
        
        async with self.http.get(CREATOR_GET_URL.format(creator=creator_id)) as response:
            response.raise_for_status()
            creator = hoordu.Dynamic.from_json(await response.text()).body
        
        options.creator = creator_id
        options.pixiv_id = creator.user.userId
        
        related_urls = creator.profileLinks
        related_urls.append(PIXIV_URL.format(pixiv_id=options.pixiv_id))
        
        return SearchDetails(
            hint=creator.creatorId,
            title=creator.user.name,
            description=creator.description,
            thumbnail_url=creator.user.iconUrl,
            related_urls=creator.profileLinks
        )

Plugin = Fanbox


