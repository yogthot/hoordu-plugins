#!/usr/bin/env python3

# these options are appended to the end of image urls when downloading
THUMB_SIZE = 'small'
ORIG_SIZE = 'orig'

PAGE_LIMIT = 200


import os
import re
import json
from datetime import datetime
from tempfile import mkstemp
import shutil
from urllib.parse import urlparse
import functools
import urllib3
http = urllib3.PoolManager()

import hoordu
from hoordu.models import *
from hoordu.plugins import *
from hoordu.forms import *

from requests_oauthlib import OAuth1Session
import twitter

OAUTH_REQUEST_TOKEN_URL = 'https://api.twitter.com/oauth/request_token'
OAUTH_ACCESS_TOKEN_URL = 'https://api.twitter.com/oauth/access_token'
OAUTH_AUTHORIZATION_URL = 'https://api.twitter.com/oauth/authorize'

TWEET_FORMAT = 'https://twitter.com/{user}/status/{tweet_id}'
TWEET_REGEXP = re.compile('^https?:\/\/twitter\.com\/(?P<user>[^\/]+)\/status\/(?P<tweet_id>\d+)(?:\/.*)?(?:\?.*)?$', flags=re.IGNORECASE)
TIMELINE_REGEXP = re.compile('^https?:\/\/twitter\.com\/(?P<user>[^\/]+)(?:\/(?P<type>[^\/]+)?)?(?:\?.*)?$', flags=re.IGNORECASE)

MEDIA_URL = '{base_url}?format={ext}&name={size}'

def oauth_start(consumer_key, consumer_secret):
    oauth_client = OAuth1Session(consumer_key, client_secret=consumer_secret, callback_uri='oob')
    
    resp = oauth_client.fetch_request_token(OAUTH_REQUEST_TOKEN_URL)
    url = oauth_client.authorization_url(OAUTH_AUTHORIZATION_URL)
    
    oauth_token = resp.get('oauth_token')
    oauth_token_secret = resp.get('oauth_token_secret')
    
    return oauth_token, oauth_token_secret, url

def oauth_finish(consumer_key, consumer_secret, oauth_token, oauth_token_secret, pin):
    oauth_client = OAuth1Session(consumer_key, client_secret=consumer_secret,
                                resource_owner_key=oauth_token,
                                resource_owner_secret=oauth_token_secret,
                                verifier=pin)
    
    try:
        resp = oauth_client.fetch_access_token(OAUTH_ACCESS_TOKEN_URL)
    except ValueError as e:
        raise APIError('invalid response from Twitter when requesting temp token') from e
        
    access_token_key = resp.get('oauth_token')
    access_token_secret = resp.get('oauth_token_secret')
    
    return access_token_key, access_token_secret

def unwind_url(url):
    final_url = url
    try:
        while url is not None:
            resp = http.request('HEAD', url, redirect=False, timeout=10)
            if resp.status // 100 == 3:
                url = resp.headers.get('Location')
                if url is not None:
                    final_url = url
            else:
                url = None
    except:
        pass
    
    return final_url

class TweetIterator(BaseIterator):
    def __init__(self, twitter, subscription=None, options=None):
        super().__init__(twitter, subscription=subscription, options=options)
        
        self.api = twitter.api
        self.log = twitter.log
        
        self.options.user_id = self.options.get('user_id')
        
        self.first_id = None
        self.state.head_id = self.state.get('head_id')
        self.state.tail_id = self.state.get('tail_id')
    
    def init(self):
        if self.options.user_id is None:
            user = self.api.GetUser(screen_name=self.options.user)
            
            self.options.user_id = user.id_str
            
            if self.subscription is not None:
                self.subscription.options = self.options.to_json()
                self.twitter.core.add(self.subscription)
    
    def _page_iterator(self, method, limit=None, max_id=None, **kwargs):
        total = 0
        while True:
            tweets = method(max_id=max_id, **kwargs)
            self.log.debug('method: %s, max_id: %s, kwargs: %s', method.__name__, max_id, kwargs)
            self.log.debug('page: %s', tweets)
            if len(tweets) == 0:
                return
            
            for tweet in tweets:
                yield tweet
                max_id = tweet.id - 1
                
                total += 1
                if limit is not None and total >= limit:
                    return
    
    def _feed_iterator(self, direction=FetchDirection.newer, limit=None):
        head = (direction == FetchDirection.newer)
        
        page_size = PAGE_LIMIT if limit is None else min(limit, PAGE_LIMIT)
        since_id = self.state.head_id if head else None
        max_id = self.state.tail_id if not head else None
        
        # max_id: Returns results with an ID less than (that is, older than) or equal to the specified ID.
        if max_id is not None:
            max_id = int(max_id) - 1
        
        if self.options.method == 'tweets':
            tweets = self._page_iterator(
                self.api.GetUserTimeline,
                limit=limit,
                user_id=self.options.user_id, count=page_size, exclude_replies=False, include_rts=False,
                max_id=max_id, since_id=since_id
            )
            
        elif self.options.method == 'retweets':
            tweets = self._page_iterator(
                self.api.GetUserTimeline,
                limit=limit,
                user_id=self.options.user_id, count=page_size, exclude_replies=False, include_rts=True,
                max_id=max_id, since_id=since_id
            )
            
        elif self.options.method == 'likes':
            tweets = self._page_iterator(
                self.api.GetFavorites,
                limit=limit,
                user_id=self.options.user_id, count=page_size,
                max_id=max_id, since_id=since_id
            )
        
        else:
            tweets = []
        
        return tweets
    
    def _tweet_has_content(self, tweet):
        if tweet.retweeted_status is not None:
            tweet = tweet.retweeted_status
        
        return ((
            tweet.media is not None and
            len(tweet.media) > 0
        ) or (
            tweet.urls is not None and
            len(tweet.urls) > 0
        ))
    
    def fetch(self, direction=FetchDirection.newer, n=None):
        limit = n
        if direction == FetchDirection.newer:
            if self.state.tail_id is None:
                direction = FetchDirection.older
            else:
                limit = None
        
        tweets = self._feed_iterator(direction, limit=limit)
        
        first_iteration = True
        for tweet in tweets:
            if first_iteration and (self.state.head_id is None or direction == FetchDirection.newer):
                self.first_id = tweet.id_str
            
            if self._tweet_has_content(tweet):
                remote_post = self.plugin.tweet_to_remote_post(tweet, preview=self.subscription is None)
                yield remote_post
                
                if self.subscription is not None:
                    self.subscription.feed.append(remote_post)
                
                self.plugin.core.commit()
            
            if direction == FetchDirection.older:
                self.state.tail_id = tweet.id_str
            
            first_iteration = False
        
        if self.first_id is not None:
            self.state.head_id = self.first_id
            self.first_id = None
        
        if self.subscription is not None:
            self.subscription.state = self.state.to_json()
            self.plugin.core.add(self.subscription)

class Twitter(BasePlugin):
    name = 'twitter'
    version = 3
    iterator = TweetIterator
    
    @classmethod
    def config_form(cls):
        return Form('{} config'.format(cls.name),
            ('consumer_key', Input('consumer key', [validators.required])),
            ('consumer_secret', Input('consumer secret', [validators.required])),
            ('access_token_key', Input('access token key')),
            ('access_token_secret', Input('access token secret'))
        )
    
    @classmethod
    def init(cls, core, parameters=None):
        source = core.source
        
        # check if everything is ready to use
        config = hoordu.Dynamic.from_json(source.config)
        
        if not config.defined('consumer_key', 'consumer_secret'):
            # try to get the values from the parameters
            if parameters is not None:
                config.update(parameters)
                
                source.config = json.dumps(config)
                core.add(source)
        
        if not config.defined('consumer_key', 'consumer_secret'):
            # but if they're still None, the api can't be used
            return False, cls.config_form()
        
        elif not config.defined('access_token_key', 'access_token_secret'):
            pin = None
            if parameters is not None:
                pin = parameters.get('pin')
            
            if pin is None:
                oauth_token, oauth_token_secret, url = oauth_start(config.consumer_key, config.consumer_secret)
                
                config.oauth_token = oauth_token
                config.oauth_token_secret = oauth_token_secret
                source.config = config.to_json()
                core.add(source)
                
                oauth_form = Form('twitter authentication',
                    Label('please login to twitter via this url to get your pin:\n{}'.format(url)),
                    ('pin', Input('pin', [validators.required]))
                )
                
                return False, oauth_form
                
            else:
                oauth_token = config.pop('oauth_token')
                oauth_token_secret = config.pop('oauth_token_secret')
                
                access_token_key, access_token_secret = oauth_finish(
                    config.consumer_key, config.consumer_secret,
                    oauth_token, oauth_token_secret,
                    pin)
                    
                config.access_token_key = access_token_key
                config.access_token_secret = access_token_secret
                source.config = config.to_json()
                core.add(source)
                
                return True, cls(core)
            
        else:
            # the config contains every required property
            return True, cls(core)
    
    @classmethod
    def update(cls, core):
        source = core.source
        
        if source.version < cls.version:
            # update anything if needed
            
            # if anything was updated, then the db entry should be updated as well
            source.version = cls.version
            core.add(source)
    
    def __init__(self, core, config=None):
        super().__init__(core, config)
        
        self._init_api()
    
    def _init_api(self):
        self.api = twitter.Api(
            consumer_key=self.config.consumer_key,
            consumer_secret=self.config.consumer_secret,
            access_token_key=self.config.get('access_token_key', None),
            access_token_secret=self.config.get('access_token_secret', None),
            tweet_mode='extended'
        )
    
    def parse_url(self, url):
        if url.isdigit():
            return url
        
        match = TWEET_REGEXP.match(url)
        if match:
            return match.group('tweet_id')
        
        match = TIMELINE_REGEXP.match(url)
        if match:
            user = match.group('user')
            method = match.group('type')
            
            if method != 'likes':
                method = 'tweets'
            
            return hoordu.Dynamic({
                'user': user,
                'method': method
            })
        
        return None
    
    def _download_file(self, url, filename=None):
        # TODO file downloads should be managed by hoordu
        # so that rate limiting and a download manager can be
        # implemented easily and in a centralized way
        self.log.debug('downloading %s', url)
        
        if filename is not None:
            suffix = '-{}'.format(filename)
            
        else:
            suffix = os.path.splitext(urlparse(url).path)[-1]
            if not suffix.startswith('.'):
                suffix = ''
        
        fd, path = mkstemp(suffix=suffix)
        
        with http.request('GET', url, preload_content=False) as resp, \
                os.fdopen(fd, 'w+b') as file:
            resp.read = functools.partial(resp.read, decode_content=True)
            shutil.copyfileobj(resp, file)
        
        return path
    
    def _download_video(self, media):
        variants = media.video_info.get('variants', [])
        
        variant = max(
            [v for v in variants if 'bitrate' in v],
            key=lambda v: v['bitrate'],
            default=None
        )
        
        if variant is not None:
            return self._download_file(variant['url'])
        else:
            return None
    
    def _download_media(self, media, thumbnail=False, file=False):
        thumb = None
        orig = None
        
        base_url, ext = media.media_url.rsplit('.', 1)
        filename = '{}.{}'.format(base_url.rsplit('/', 1)[-1], ext)
        
        if media.type == 'photo':
            if thumbnail:
                thumb = self._download_file(MEDIA_URL.format(base_url=base_url, ext=ext, size=THUMB_SIZE), filename)
            
            if file:
                orig = self._download_file(MEDIA_URL.format(base_url=base_url, ext=ext, size=ORIG_SIZE), filename)
            
        elif media.type == 'video' or media.type == 'animated_gif':
            if thumbnail:
                thumb = self._download_file(MEDIA_URL.format(base_url=base_url, ext=ext, size=THUMB_SIZE), filename)
            
            if file:
                orig = self._download_video(media)
        
        return thumb, orig
    
    def tweet_to_remote_post(self, tweet, remote_post=None, preview=False):
        # get the original tweet if this is a retweet
        if tweet.retweeted_status is not None:
            tweet = tweet.retweeted_status
        
        original_id = tweet.id_str
        user = tweet.user.screen_name
        user_id = tweet.user.id_str
        text = tweet.full_text
        post_time = datetime.utcfromtimestamp(tweet.created_at_in_seconds)
        
        self.log.info('getting tweet %s', original_id)
        
        if remote_post is None:
            remote_post = self.session.query(RemotePost).filter(RemotePost.source_id == self.source.id, RemotePost.original_id == original_id).one_or_none()
            if remote_post is None:
                self.log.info('creating new post')
                remote_post = RemotePost(
                    source=self.source,
                    original_id=original_id,
                    url=TWEET_FORMAT.format(user=user, tweet_id=original_id),
                    comment=text,
                    type=PostType.set,
                    post_time=post_time,
                    metadata_=json.dumps({'user': user})
                )
                
                user_tag = self.core.get_remote_tag(TagCategory.artist, user_id)
                remote_post.tags.append(user_tag)
                
                if user_tag.update_metadata('user', user):
                    self.core.add(user_tag)
                
                if tweet.favorited is True:
                    remote_post.favorite = True
                
                if tweet.possibly_sensitive:
                    nsfw_tag = self.core.get_remote_tag(TagCategory.meta, 'nsfw')
                    remote_post.tags.append(nsfw_tag)
                
                if tweet.hashtags is not None:
                    for hashtag in tweet.hashtags:
                        tag = hashtag.text
                        nsfw_tag = self.core.get_remote_tag(TagCategory.general, tag)
                        remote_post.tags.append(nsfw_tag)
                
                if tweet.in_reply_to_status_id is not None:
                    url = TWEET_FORMAT.format(user=tweet.in_reply_to_screen_name, tweet_id=tweet.in_reply_to_status_id)
                    remote_post.related.append(Related(url=url))
                
                if tweet.urls is not None:
                    for url in tweet.urls:
                        # the unwound section is a premium feature
                        self.log.info('found url %s', url.url)
                        final_url = unwind_url(url.url)
                        remote_post.related.append(Related(url=final_url))
                
                self.core.add(remote_post)
                
            else:
                self.log.info('post already exists: %s', remote_post.id)
        
        if tweet.media is not None:
            available = set(range(len(tweet.media)))
            present = set(file.remote_order for file in remote_post.files)
            
            for order in available - present:
                file = File(remote=remote_post, remote_order=order)
                self.core.add(file)
                self.core.flush()
                self.log.info('found new file for post %s, file order: %s', remote_post.id, order)
            
            for file in remote_post.files:
                need_thumb = not file.thumb_present
                need_file = not file.present and not preview
                
                if need_thumb or need_file:
                    self.log.info('downloading files for post: %s, order: %r', remote_post.id, file.remote_order)
                    thumb, orig = self._download_media(tweet.media[file.remote_order], thumbnail=need_thumb, file=need_file)
                    self.core.import_file(file, orig=orig, thumb=thumb, move=True)
        
        return remote_post
    
    def download(self, url=None, remote_post=None, preview=False):
        if url is None and remote_post is None:
            raise ValueError('either url or remote_post must be passed')
        
        if remote_post is not None:
            tweet_id = remote_post.original_id
            self.log.info('update request for %s', tweet_id)
            
        else:
            self.log.info('download request for %s', url)
            if url.isdigit():
                tweet_id = url
                
            else:
                match = TWEET_REGEXP.match(url)
                if not match:
                    raise ValueError('unsupported url: {}'.format(repr(url)))
                
                tweet_id = match.group('tweet_id')
        
        tweet = self.api.GetStatus(tweet_id)
        self.log.debug('tweet: %s', tweet)
        
        return self.tweet_to_remote_post(tweet, remote_post=remote_post, preview=preview)
    
    def search_form(self):
        return Form('{} search'.format(self.name),
            ('method', ChoiceInput('method', [
                    ('tweets', 'tweets'),
                    ('retweets', 'retweets'),
                    ('likes', 'likes')
                ], [validators.required()])),
            ('user', Input('screen name', [validators.required()]))
        )

Plugin = Twitter


