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
import urllib3
http = urllib3.PoolManager()

import hoordu
from hoordu.models import *
from hoordu.plugins import *

from requests_oauthlib import OAuth1Session
import twitter

OAUTH_REQUEST_TOKEN_URL = 'https://api.twitter.com/oauth/request_token'
OAUTH_ACCESS_TOKEN_URL = 'https://api.twitter.com/oauth/access_token'
OAUTH_AUTHORIZATION_URL = 'https://api.twitter.com/oauth/authorize'

TWEET_FORMAT = 'https://twitter.com/{user}/status/{tweet_id}'
TWEET_REGEXP = re.compile('^https?:\/\/twitter.com\/(?P<user>[^\/]+)\/status\/(?P<tweet_id>\d+)(?:/.*)?(?:\?.*)?$')

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
        raise ValueError('Invalid response from Twitter requesting temp token') from e
        
    access_token_key = resp.get('oauth_token')
    access_token_secret = resp.get('oauth_token_secret')
    
    return access_token_key, access_token_secret

def unwind_url(url):
    final_url = url
    try:
        while url is not None:
            resp = http.request('HEAD', url, redirect=False)
            if resp.status // 100 == 3:
                url = resp.headers.get('Location')
                if url is not None:
                    final_url = url
            else:
                url = None
    except:
        pass
    
    return final_url

class TweetIterator:
    def __init__(self, twitter, subscription=None, options=None):
        self.twitter = twitter
        self.api = twitter.api
        self.log = twitter.log
        self.subscription = subscription
        
        if self.subscription is not None:
            options = hoordu.Settings.from_json(self.subscription.options)
            self.state = hoordu.Settings.from_json(self.subscription.state)
        else:
            self.state = {}
        
        if options is None or 'method' not in options or 'user' not in options:
            raise ValueError('search options are invalid: {}'.format(options))
        
        self.method = options['method']
        self.user = options['user']
        
        self.head_id = self.state.get('head_id')
        self.tail_id = self.state.get('tail_id')
    
    def _save_state(self):
        self.state['head_id'] = self.head_id
        self.state['tail_id'] = self.tail_id
        if self.subscription is not None:
            self.subscription.state = json.dumps(self.state)
    
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
        since_id = self.head_id if head else None
        max_id = self.tail_id if not head else None
        
        # max_id: Returns results with an ID less than (that is, older than) or equal to the specified ID.
        if max_id is not None:
            max_id = int(max_id) - 1
        
        if self.method == 'tweets':
            tweets = self._page_iterator(
                self.api.GetUserTimeline,
                limit=limit,
                screen_name=self.user, count=page_size, exclude_replies=False, include_rts=False,
                max_id=max_id, since_id=since_id
            )
            
        elif self.method == 'retweets':
            tweets = self._page_iterator(
                self.api.GetUserTimeline,
                limit=limit,
                screen_name=self.user, count=page_size, exclude_replies=False, include_rts=True,
                max_id=max_id, since_id=since_id
            )
            
        elif self.method == 'likes':
            tweets = self._page_iterator(
                self.api.GetFavorites,
                limit=limit,
                screen_name=self.user, count=page_size,
                max_id=max_id, since_id=since_id
            )
        
        else:
            tweets = []
        
        return tweets
    
    def fetch(self, direction=FetchDirection.newer, n=None):
        """
        Try to get at least `n` newer or older posts from this search
        depending on the direction.
        Create a RemotePost entry and any associated Files for each post found,
        thumbnails should be downloaded, files are optional.
        Posts should always come ordered in the same way.
        
        Returns a list of the new RemotePost objects.
        """
        
        limit = n
        if direction == FetchDirection.newer:
            if self.tail_id is None:
                direction == FetchDirection.older
            else:
                limit = None
        
        tweets = self._feed_iterator(direction, limit=limit)
        
        posts = []
        first_iteration = True
        for tweet in tweets:
            if first_iteration and (self.head_id is None or direction == FetchDirection.newer):
                self.head_id = tweet.id_str
            
            if direction == FetchDirection.older:
                self.tail_id = tweet.id_str
            
            post = self.twitter.tweet_to_remote_post(tweet, preview=self.subscription is None)
            posts.append(post)
            
            if self.subscription is not None:
                self.subscription.feed.append(post)
            
            # always commit changes
            # RemotePost, RemoteTag and the subscription feed are simply a cache
            # the file downloads are more expensive than a call to the database
            self.twitter.core.commit()
            
            first_iteration = False
        
        self._save_state()
        if self.subscription is not None:
            self.twitter.core.add(self.subscription)
        
        return posts

class Twitter:
    name = 'twitter'
    version = 1
    
    _config_keys = ['consumer_key', 'consumer_secret', 'access_token_key', 'access_token_secret']
    @classmethod
    def init(cls, core, parameters=None):
        source = core.source
        
        cls.update(core)
        
        # check if everything is ready to use
        config = hoordu.Settings.from_json(source.config)
        
        if not config.contains('consumer_key', 'consumer_secret'):
            # try to get the values from the parameters
            if parameters is not None:
                config.update((k, parameters[k]) for k in cls._config_keys if k in parameters)
                
                source.config = json.dumps(config)
                core.add(source)
        
        
        if not config.contains('consumer_key', 'consumer_secret'):
            # but if they're still None, the api can't be used
            source.setup_state = SourceSetupState.config
            core.add(source)
            #core.commit()
            # request the values to be sent into parameters
            return False, None
        
        elif not config.contains('access_token_key', 'access_token_secret'):
            pin = None
            if parameters is not None:
                pin = parameters.get('user_input')
            
            if pin is None:
                oauth_token, oauth_token_secret, url = oauth_start(config.consumer_key, config.consumer_secret)
                
                config.oauth_token = oauth_token
                config.oauth_token_secret = oauth_token_secret
                source.setup_state = SourceSetupState.setup
                source.config = config.to_json()
                core.add(source)
                
                return False, url
                
            else:
                oauth_token = config.pop('oauth_token')
                oauth_token_secret = config.pop('oauth_token_secret')
                
                access_token_key, access_token_secret = oauth_finish(
                    config.consumer_key, config.consumer_secret,
                    oauth_token, oauth_token_secret,
                    pin)
                    
                config.access_token_key = access_token_key
                config.access_token_secret = access_token_secret
                source.setup_state = SourceSetupState.ready
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
        self.core = core
        self.source = core.source
        self.log = core.logger
        self.session = core.session
        
        if config is None:
            config = hoordu.Settings.from_json(self.source.config)
        
        self._load_config(config)
        
        self._init_api()
    
    def _load_config(self, config):
        self.consumer_key = config.consumer_key
        self.consumer_secret = config.consumer_secret
        
        self.access_token_key = config.get('access_token_key', None)
        self.access_token_secret = config.get('access_token_secret', None)
    
    def _init_api(self):
        self.api = twitter.Api(
            consumer_key=self.consumer_key,
            consumer_secret=self.consumer_secret,
            access_token_key=self.access_token_key,
            access_token_secret=self.access_token_secret,
            tweet_mode='extended'
        )
    
    def can_download(self, url):
        """
        Checks if an url can be downloaded by this plugin.
        
        Returns True if this plugin is able to download the url.
        """
        
        is_valid_url = bool(TWEET_REGEXP.match(url))
        is_digit = url.isdigit()
        return is_valid_url or is_digit
    
    def _download_file(self, url):
        # TODO file downloads should be managed by hoordu
        # so that rate limiting and a download manager can be
        # implemented easily and in a centralized way
        self.log.debug('downloading %s', url)
        
        suffix = os.path.splitext(urlparse(url).path)[-1].split(':')[0]
        if not suffix.startswith('.'):
            suffix = ''
        
        fd, path = mkstemp(suffix=suffix)
        
        with http.request('GET', url, preload_content=False) as resp, \
                os.fdopen(fd, 'w+b') as file:
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
        
        if media.type == 'photo':
            if thumbnail:
                thumb = self._download_file('{}:{}'.format(media.media_url, THUMB_SIZE))
            
            if file:
                orig = self._download_file('{}:{}'.format(media.media_url, ORIG_SIZE))
            
        elif media.type == 'video' or media.type == 'animated_gif':
            if thumbnail:
                thumb = self._download_file('{}:{}'.format(media.media_url, THUMB_SIZE))
            
            if file:
                orig = self._download_video(media)
        
        return thumb, orig
    
    def tweet_to_remote_post(self, tweet, remote_post=None, preview=False):
        # get the original tweet if this is a retweet
        if tweet.retweeted_status is not None:
            tweet = tweet.retweeted_status
        
        original_id = tweet.id_str
        user = tweet.user.screen_name
        text = tweet.full_text
        post_time = datetime.utcfromtimestamp(tweet.created_at_in_seconds)
        
        self.log.info('getting tweet %s', original_id)
        
        if remote_post is not None:
            post = remote_post
        
        else:
            post = self.session.query(RemotePost).filter(RemotePost.source_id == self.source.id, RemotePost.original_id == original_id).one_or_none()
            if post is None:
                self.log.info('creating new post')
                post = RemotePost(
                    source=self.source,
                    original_id=original_id,
                    url=TWEET_FORMAT.format(user=user, tweet_id=original_id),
                    comment=text,
                    type=PostType.set,
                    post_time=post_time,
                    metadata_=json.dumps({'user': user})
                )
                
                user_tag = self.core.get_remote_tag(source=self.source, category=TagCategory.artist, tag=user)
                post.tags.append(user_tag)
                
                if tweet.favorited is True:
                    post.favorite = True
                
                if tweet.possibly_sensitive:
                    nsfw_tag = self.core.get_remote_tag(source=self.source, category=TagCategory.meta, tag='nsfw')
                    post.tags.append(nsfw_tag)
                
                if tweet.hashtags is not None:
                    for hashtag in tweet.hashtags:
                        tag = hashtag.text
                        nsfw_tag = self.core.get_remote_tag(source=self.source, category=TagCategory.general, tag=tag)
                        post.tags.append(nsfw_tag)
                
                if tweet.in_reply_to_status_id is not None:
                    url = TWEET_FORMAT.format(user=tweet.in_reply_to_screen_name, tweet_id=tweet.in_reply_to_status_id)
                    post.related.append(Related(url=url))
                
                if tweet.urls is not None:
                    for url in tweet.urls:
                        # the unwound section is a premium feature
                        self.log.info('found url %s', url.url)
                        final_url = unwind_url(url.url)
                        post.related.append(Related(url=final_url))
                
                self.core.add(post)
                
            else:
                self.log.info('post already exists: %s', post.id)
        
        if tweet.media is not None:
            available = set(range(len(tweet.media)))
            present = set(file.remote_order for file in post.files)
            
            for order in available - present:
                file = File(remote=post, remote_order=order)
                self.core.add(file)
                self.core.flush()
                self.log.info('found new file for post %s, file order: %s', post.id, order)
            
            for file in post.files:
                need_thumb = not file.thumb_present
                need_file = not file.present and not preview
                
                if need_thumb or need_file:
                    self.log.info('downloading files for post: %s, file: %r, thumb: %r', post.id, need_file, need_thumb)
                    thumb, orig = self._download_media(tweet.media[file.remote_order], thumbnail=need_thumb, file=need_file)
                    self.core.import_file(file, orig=orig, thumb=thumb, move=True)
        
        return post
    
    def download(self, url=None, remote_post=None, preview=False):
        """
        Creates or updates a RemotePost entry along with all the associated Files,
        and downloads all files and thumbnails that aren't present yet.
        
        If remote_post is passed, its original_id will be used and it will be
        updated in place.
        
        If preview is set to True, then only the thumbnails are downloaded.
        
        Returns the downloaded RemotePost object.
        """
        
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
    
    _supported_methods = ['tweets', 'retweets', 'likes']
    def create_subscription(self, name, options=None, iterator=None):
        """
        Creates a Subscription entry for the given search options identified by the given name,
        should not get any posts from the post source.
        """
        
        if options is not None:
            method, user = options.split(':')
            
            if method not in self._supported_methods:
                raise ValueError('unsupported method: {}'.format(repr(method)))
            
            opts = {
                'method': method,
                'user': user
            }
            state = {}
            
            
        elif iterator is not None:
            opts = {
                'method': iterator.method,
                'user': iterator.user
            }
            state = iterator.state
        
        sub = Subscription(
            source=self.source,
            name=name,
            options=json.dumps(opts),
            state=json.dumps(state)
        )
        
        self.core.add(sub)
        self.core.flush()
        
        return sub
    
    def search(self, options):
        """
        Creates a temporary search for a given set of search options.
        
        Returns a post iterator object.
        """
        
        method, user = options.split(':')
        
        if method not in self._supported_methods:
            raise ValueError('unsupported method: {}'.format(repr(method)))
        
        options = {
            'method': method,
            'user': user
        }
        
        return TweetIterator(self, options=options)
    
    def get_iterator(self, subscription):
        """
        Gets the post iterator for a specific subscription.
        
        Returns a post iterator object.
        """
        
        return TweetIterator(self, subscription=subscription)

Plugin = Twitter
