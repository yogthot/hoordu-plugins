import os
import re
from datetime import datetime
from tempfile import mkstemp
import shutil
from urllib.parse import urlparse
import functools
from collections import OrderedDict
import dateutil.parser

import aiohttp
import contextlib

import hoordu
from hoordu.models import *
from hoordu.plugins import *
from hoordu.forms import *
from hoordu.oauth.client import *

AUTH_URL = 'https://twitter.com/i/oauth2/authorize'
TOKEN_URL = 'https://api.twitter.com/2/oauth2/token'
REDIRECT_URL = 'http://127.0.0.1:8941/twitter'
SCOPES = 'tweet.read users.read bookmark.read offline.access'
CHALLENGE_MODE = 'plain'

TWEET_FORMAT = 'https://twitter.com/{user}/status/{tweet_id}'
TWEET_REGEXP = [
    re.compile('^https?:\/\/twitter\.com\/(?P<user>[^\/]+)\/status\/(?P<tweet_id>\d+)(?:\/.*)?(?:\?.*)?$', flags=re.IGNORECASE),
    re.compile('^https?:\/\/twitter\.com\/i\/web\/status\/(?P<tweet_id>\d+)(?:\/.*)?(?:\?.*)?$', flags=re.IGNORECASE)
]
TIMELINE_REGEXP = re.compile('^https?:\/\/twitter\.com\/(?P<user>[^\/]+)(?:\/(?P<type>[^\/]+)?)?(?:\?.*)?$', flags=re.IGNORECASE)

PROFILE_IMAGE_REGEXP = re.compile('^(?P<base>.+_)(?P<size>[^\.]+)(?P<ext>.+)$')

NEW_MEDIA_URL = '{base_url}?format={ext}&name={size}'
OLD_MEDIA_URL = '{base_url}.{ext}:{size}'
MEDIA_URL = NEW_MEDIA_URL

# these options are appended to the end of image urls when downloading
THUMB_SIZE = 'small'
ORIG_SIZE = 'orig'
PROFILE_THUMB_SIZE = '200x200'

PAGE_LIMIT = 50 # 5 to 100

class TwitterClient:
    def __init__(self, access_token, refresh_token_cb=None, app_auth=False):
        self.access_token = access_token
        self.refresh_token_cb = refresh_token_cb
        self.http = aiohttp.ClientSession()
        self.http.headers['Authorization'] = f'Bearer {access_token}'
    
    async def __aenter__(self) -> 'TwitterClient':
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
    
    async def get_user(self, user_id=None, *, username=None, full=False):
        params = {
            'user.fields': 'id,name,username',
        }
        if user_id is not None:
            url = f'https://api.twitter.com/2/users/{user_id}'
            
        elif username is not None:
            url = 'https://api.twitter.com/2/users/by'
            params['usernames'] = username
        
        if full:
            params['user.fields'] = 'id,name,username,created_at,description,profile_image_url,verified,entities'
        
        async with self._get(url, params=params) as resp:
            return hoordu.Dynamic.from_json(await resp.text())
    
    async def get_tweet(self, tweet_id):
        params = {
            'ids': str(tweet_id),
            'expansions': 'author_id,attachments.media_keys,referenced_tweets.id,referenced_tweets.id.author_id,in_reply_to_user_id',
            'tweet.fields': 'attachments,author_id,created_at,entities,id,lang,possibly_sensitive,'
                            'public_metrics,referenced_tweets,source,text,withheld,in_reply_to_user_id',
            'user.fields': 'id,name,username',
            'media.fields': 'media_key,type,url,preview_image_url,variants',
        }
        url = 'https://api.twitter.com/2/tweets'
        
        async with self._get(url, params=params) as resp:
            return hoordu.Dynamic.from_json(await resp.text())
    
    async def get_timeline(self, user_id, **kwargs):
        params = {
            'expansions': 'author_id,attachments.media_keys,referenced_tweets.id,referenced_tweets.id.author_id,in_reply_to_user_id',
            'tweet.fields': 'attachments,author_id,created_at,entities,id,lang,possibly_sensitive,'
                            'public_metrics,referenced_tweets,source,text,withheld,in_reply_to_user_id',
            'user.fields': 'id,name,username',
            'media.fields': 'media_key,type,url,preview_image_url,variants',
            'max_results': str(PAGE_LIMIT),
        }
        params.update(kwargs)
        url = f'https://api.twitter.com/2/users/{user_id}/tweets'
        
        async with self._get(url, params=params) as resp:
            return hoordu.Dynamic.from_json(await resp.text())


class IncludesMap(OrderedDict):
    def __init__(self, includes_obj):
        super().__init__()
        
        self._add_section(includes_obj, 'users', 'id')
        self._add_section(includes_obj, 'tweets', 'id')
        self._add_section(includes_obj, 'media', 'media_key')
    
    def _add_section(self, includes_obj, key, id_key):
        if key in includes_obj:
            for include in includes_obj[key]:
                self[(key, include[id_key])] = include


class TweetIterator(IteratorBase['Twitter']):
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
            user = await self.api.get_user(username=self.options.user)
            
            self.options.user_id = user.id
            
            if self.subscription is not None:
                self.subscription.options = self.options.to_json()
                self.session.add(self.subscription)
    
    def reconfigure(self, direction=FetchDirection.newer, num_posts=None):
        if direction == FetchDirection.newer:
            if self.state.tail_id is None:
                direction = FetchDirection.older
            else:
                num_posts = None
        
        super().reconfigure(direction=direction, num_posts=num_posts)
    
    async def _feed_iterator(self):
        limit = self.num_posts
        
        #since_id=<get tweets more recent than this, exclude>
        #until_id=<get tweets older than this, exclude>
        
        #pagination_token=body.page.meta.next_token can be used when iterating, but not from state
        
        kwargs = {}
        
        if self.direction == FetchDirection.newer:
            if self.state.head_id is not None:
                kwargs['since_id'] = self.state.head_id
            
        else:
            if self.state.tail_id is not None:
                kwargs['until_id'] = self.state.tail_id
        
        if self.options.method != 'retweets':
            kwargs['exclude'] = 'retweets'
        
        total = 0
        while True:
            page_size = PAGE_LIMIT if limit is None else max(min(limit - total, PAGE_LIMIT), 5)
            kwargs['max_results'] = str(page_size)
            
            self.log.info('getting next page')
            body = await self.api.get_timeline(self.options.user_id, **kwargs)
            
            if not body.contains('data'):
                return
            
            tweets = body.data
            includes = IncludesMap(body.includes)
            
            for tweet in tweets:
                yield tweet, includes
                kwargs['until_id'] = tweet.id
                
                total += 1
                if limit is not None and total >= limit:
                    return
    
    def _tweet_has_content(self, tweet, includes):
        retweeted_id = self.plugin._referenced_tweet_id(tweet, 'retweeted')
        if retweeted_id is not None:
            tweet = includes['tweets', retweeted_id]
        
        media_keys = tweet.get_path('attachments', 'media_keys')
        urls = tweet.get_path('entities', 'urls')
        
        return ((
            media_keys is not None and
            len(media_keys) > 0
        ) or (
            urls is not None and
            len(urls) > 0
        ))
    
    async def generator(self):
        first_iteration = True
        
        async for tweet, includes in self._feed_iterator():
            if first_iteration and (self.state.head_id is None or self.direction == FetchDirection.newer):
                self.first_id = tweet.id
            
            if self._tweet_has_content(tweet, includes):
                remote_post = await self.plugin._to_remote_post(tweet, includes, preview=self.subscription is None)
                yield remote_post
                
                if self.subscription is not None:
                    await self.subscription.add_post(remote_post)
                
                await self.session.commit()
            
            if self.direction == FetchDirection.older:
                self.state.tail_id = tweet.id
            
            first_iteration = False
        
        if self.first_id is not None:
            self.state.head_id = self.first_id
            self.first_id = None
        
        if self.subscription is not None:
            self.subscription.state = self.state.to_json()
            self.session.add(self.subscription)
        
        await self.session.commit()


class Twitter(SimplePlugin):
    name = 'twitter'
    version = 4
    iterator = TweetIterator
    
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
                'redirect_uri': REDIRECT_URL, # TODO move redirect uri to base class
                'scopes': SCOPES,
                'client_id': config.client_id,
                'client_secret': config.client_secret,
                'code_challenge_method': CHALLENGE_MODE,
            })
            
            if code is None:
                url, state, challenge = oauth.auth_url(use_state=True, use_code_verifier=True)
                
                config.state = state
                config.challenge = challenge
                plugin.config = config.to_json()
                session.add(plugin)
                
                return False, OAuthForm('twitter authentication', url)
                
            else:
                response = await oauth.get_access_token(code, code_verifier=config.challenge)
                
                config.access_token = response['access_token']
                config.refresh_token = response['refresh_token']
                config.pop('state')
                config.pop('challenge')
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
        if url.isdigit():
            return url
        
        for regexp in TWEET_REGEXP:
            match = regexp.match(url)
            if match:
                return match.group('tweet_id')
        
        match = TIMELINE_REGEXP.match(url)
        if match:
            user = match.group('user')
            method = match.group('type')
            
            #if method != 'likes':
            #    method = 'tweets'
            method = 'tweets'
            
            return hoordu.Dynamic({
                'user': user,
                'method': method
            })
        
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
                'client_secret': self.config.client_secret,
                'code_challenge_method': CHALLENGE_MODE,
            })
            
            async with TwitterClient(self.config.access_token, self._refresh_token) as api:
                self.api: TwitterClient = api
                yield self
    
    # TODO maybe move this to a base class
    async def _refresh_token(self):
        session = self.session.priority
        plugin = await self.get_plugin(session)
        config = hoordu.Dynamic.from_json(plugin.config)
        
        try:
            self.log.info('attempting to refresh access token')
            tokens = await self.oauth.refresh_access_token(config.refresh_token)
            
        except OAuthError as e:
            self.log.warning('refresh token was invalid')
            await self.session.rollback()
            
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
    
    async def _unwind_url(self, url, iterations=20):
        final_url = url
        scheme = re.compile('^https?:\/\/')
        
        i = 0
        try:
            while url is not None:
                async with self.api.http.request('HEAD', url, allow_redirects=False, timeout=10) as resp:
                    if resp.status // 100 == 3:
                        # check if relative url, append previous domain
                        location = resp.headers.get('Location')
                        
                        if not scheme.match(location):
                            if location.startswith('/'):
                                # same domain absolute redirect
                                match = scheme.match(url)
                                main = url[:url.find('/', len(match[0]))]
                                url = main + location
                                
                            else:
                                # same domain relative redirect
                                main = url[:url.rfind('/') + 1]
                                url = main + location
                            
                        else:
                            # different domain redirect
                            url = location
                        
                        if url is not None:
                            final_url = url
                    else:
                        url = None
                    
                    i += 1
                    if iterations is not None and i >= iterations:
                        break
                
        except:
            pass
        
        return final_url
    
    async def _download_media_file(self, base_url, ext, size, filename=None, template=MEDIA_URL):
        url = template.format(base_url=base_url, ext=ext, size=size)
        path, _ = await self.session.download(url, suffix=filename)
        return path
    
    async def _download_video(self, media):
        variants = media.get('variants', [])
        
        variant = max(
            [v for v in variants if 'bit_rate' in v],
            key=lambda v: v['bit_rate'],
            default=None
        )
        
        if variant is not None:
            path, _ = await self.session.download(variant['url'])
            return path
        else:
            return None
    
    def _referenced_tweet_id(self, tweet, type):
        references = tweet.get('referenced_tweets')
        if references is None: return None
        return next((t.id for t in tweet.referenced_tweets if t.type == type), None)
    
    async def _to_remote_post(self, tweet, includes, remote_post=None, preview=False):
        # get the original tweet if this is a retweet
        retweeted_id = self._referenced_tweet_id(tweet, 'retweeted')
        if retweeted_id is not None:
            tweet = includes['tweets', retweeted_id]
        
        author = includes['users', tweet.author_id]
        
        original_id = tweet.id
        user = author.username
        user_id = tweet.author_id
        text = tweet.text
        post_time = dateutil.parser.isoparse(tweet.created_at).replace(tzinfo=None)
        
        if remote_post is None:
            remote_post = await self._get_post(original_id)
        
        remote_post.url = TWEET_FORMAT.format(user=user, tweet_id=original_id)
        remote_post.comment = text
        remote_post.type = PostType.set
        remote_post.post_time = post_time
        remote_post.metadata_ = hoordu.Dynamic({'user': user}).to_json()
        self.session.add(remote_post)
        
        self.log.info(f'downloading post: {remote_post.original_id}')
        self.log.info(f'local id: {remote_post.id}')
        
        user_tag = await self._get_tag(TagCategory.artist, user_id)
        await remote_post.add_tag(user_tag)
        
        if user_tag.update_metadata('user', user):
            self.session.add(user_tag)
        
        if tweet.possibly_sensitive:
            nsfw_tag = await self._get_tag(TagCategory.meta, 'nsfw')
            await remote_post.add_tag(nsfw_tag)
        
        hashtags = tweet.get_path('entities', 'hashtags')
        if hashtags is not None:
            for hashtag in hashtags:
                tag = await self._get_tag(TagCategory.general, hashtag.tag)
                await remote_post.add_tag(tag)
        
        async def add_related_tweet(type):
            related_id = self._referenced_tweet_id(tweet, type)
            if related_id is not None:
                related_tweet = includes.get(('tweets', related_id))
                if related_tweet is not None:
                    related_user = includes['users', related_tweet.author_id]
                    url = TWEET_FORMAT.format(user=related_user.username, tweet_id=related_id)
                    await remote_post.add_related_url(url)
        
        await add_related_tweet('replied_to')
        await add_related_tweet('quoted')
        
        urls = tweet.get_path('entities', 'urls')
        if urls is not None:
            for url in urls:
                # let's pretend this does not happen anymore
                #if SUPPORT_URL_REGEXP.match(url.url):
                #    raise APIError(text)
                
                #final_url = self._unwind_url(url.url)
                
                # quick way of filtering photo/quote urls from more relevant ones
                unwound = url.get('unwound_url')
                if unwound is not None:
                    await remote_post.add_related_url(unwound)
        
        self.session.add(remote_post)
        
        media_keys = tweet.get_path('attachments', 'media_keys')
        if media_keys is not None:
            media_list = [includes['media', key] for key in media_keys]
            
            available = set(range(len(media_list)))
            present = set(file.remote_order for file in await remote_post.fetch(RemotePost.files))
            
            for order in available - present:
                file = File(remote=remote_post, remote_order=order)
                self.session.add(file)
                await self.session.flush()
            
            for file in await remote_post.fetch(RemotePost.files):
                need_thumb = not file.thumb_present
                need_file = not file.present and not preview
                
                if need_thumb or need_file:
                    self.log.info(f'downloading file: {file.remote_order}')
                    
                    media = media_list[file.remote_order]
                    thumb = None
                    orig = None
                    
                    if media.type == 'photo':
                        base_url, ext = media.url.rsplit('.', 1)
                        filename = '{}.{}'.format(base_url.rsplit('/', 1)[-1], ext)
                        
                        if need_thumb:
                            thumb = await self._download_media_file(base_url, ext, THUMB_SIZE, filename)
                        
                        if need_file:
                            orig = await self._download_media_file(base_url, ext, ORIG_SIZE, filename)
                        
                        await self.session.import_file(file, orig=orig, thumb=thumb, move=True)
                        file.ext = ext
                        file.thumb_ext = ext
                        self.session.add(file)
                        
                    elif media.type == 'video' or media.type == 'animated_gif':
                        base_url, ext = media.preview_image_url.rsplit('.', 1)
                        filename = '{}.{}'.format(base_url.rsplit('/', 1)[-1], ext)
                        
                        if need_thumb:
                            thumb = await self._download_media_file(base_url, ext, THUMB_SIZE, filename)
                        
                        if need_file:
                            orig = await self._download_video(media)
                        
                        await self.session.import_file(file, orig=orig, thumb=thumb, move=True)
                        file.thumb_ext = ext
                        self.session.add(file)
        
        return remote_post
    
    async def download(self, id=None, remote_post=None, preview=False):
        if id is None and remote_post is None:
            raise ValueError('either id or remote_post must be passed')
        
        if remote_post is not None:
            id = remote_post.original_id
        
        body = await self.api.get_tweet(id)
        tweet = body.data[0]
        includes = IncludesMap(body.includes)
        
        return await self._to_remote_post(tweet, includes, remote_post=remote_post, preview=preview)
    
    def search_form(self):
        return Form('{} search'.format(self.name),
            ('method', ChoiceInput('method', [
                    ('tweets', 'tweets'),
                    ('retweets', 'retweets'),
                    ('likes', 'likes')
                ], [validators.required()])),
            ('user', Input('screen name', [validators.required()]))
        )
    
    async def get_search_details(self, options):
        user_id = options.get('user_id')
        kwargs = {'user_id': user_id} if user_id else {'username': options.user} 
        kwargs['full'] = True
        
        body = await self.api.get_user(**kwargs)
        user = body.data[0]
        options.user_id = user.id
        
        urls = user.get_path('entities', 'url', 'urls') or []
        description_urls = user.get_path('entities', 'description', 'urls') or []
        related_urls = {u.expanded_url for u in urls + description_urls}
        
        thumb_url = user.profile_image_url
        match = PROFILE_IMAGE_REGEXP.match(thumb_url)
        if match:
            thumb_url = match.group('base') + PROFILE_THUMB_SIZE + match.group('ext')
        
        return SearchDetails(
            hint=user.username,
            title=user.name,
            description=user.description,
            thumbnail_url=thumb_url,
            related_urls=related_urls
        )

Plugin = Twitter


