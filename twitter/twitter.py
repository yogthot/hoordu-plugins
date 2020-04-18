#!/usr/bin/env python3

CONSUMER_KEY = None
CONSUMER_SECRET = None

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

from hoordu.models import RemotePost, PostType, RemoteTag, TagCategory, File, Subscription, SourceSetupState
from requests_oauthlib import OAuth1Session
import twitter

OAUTH_REQUEST_TOKEN_URL = 'https://api.twitter.com/oauth/request_token'
OAUTH_ACCESS_TOKEN_URL = 'https://api.twitter.com/oauth/access_token'
OAUTH_AUTHORIZATION_URL = 'https://api.twitter.com/oauth/authorize'

# TODO try to support \/photo\/\d+ at the end
TWEET_FORMAT = 'https://twitter.com/{user}/status/{tweet_id}'
TWEET_REGEXP = re.compile('^https?:\/\/twitter.com\/(?P<user>[^\/]+)\/status\/(?P<tweet_id>\d+)(?:\?.*)?$')

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


class Twitter(object):
    name = 'twitter'
    version = 1
    
    @classmethod
    def init(cls, core, parameters=None):
        source = core.register_source(cls.name)
        
        cls.update(core, source)
        
        # check if everything is ready to use
        config = json.loads(source.config)
        consumer_key = config.get('consumer_key', CONSUMER_KEY)
        consumer_secret = config.get('consumer_secret', CONSUMER_SECRET)
        
        access_token_key = config.get('access_token_key', None)
        access_token_secret = config.get('access_token_secret', None)
        
        # TODO find a cleaner way to do this
        if consumer_key is None or consumer_secret is None:
            # try to get the values from the parameters
            if parameters is not None:
                consumer_key = parameters.get('consumer_key', None)
                consumer_secret = parameters.get('consumer_secret', None)
                access_token_key = parameters.get('access_token_key', None)
                access_token_secret = parameters.get('access_token_secret', None)
                
                # TODO need a better way to do this
                # e.g. config = {k:v for k,v in vars(parameters) if k in C}
                # though parameters might be a dict itself
                # try implementing iteration in the hoordu config loader
                config['consumer_key'] = consumer_key
                config['consumer_secret'] = consumer_secret
                config['access_token_key'] = access_token_key
                config['access_token_secret'] = access_token_secret
                
                config['sub_preview'] = parameters.get('sub_preview', None)
                
                source.config = json.dumps(config)
                core.add(source)
        
        
        if consumer_key is None or consumer_secret is None:
            # but if they're still None, the api can't be used
            source.setup_state = SourceSetupState.config
            core.add(source)
            #core.commit()
            # request the values to be sent into parameters
            return False, None
        
        elif access_token_key is None or access_token_secret is None:
            pin = None
            if parameters is not None:
                pin = parameters.get('user_input')
            
            if pin is None:
                oauth_token, oauth_token_secret, url = oauth_start(consumer_key, consumer_secret)
                
                config['oauth_token'] = oauth_token
                config['oauth_token_secret'] = oauth_token_secret
                source.setup_state = SourceSetupState.setup
                source.config = json.dumps(config)
                core.add(source)
                
                return False, url
                
            else:
                oauth_token = config['oauth_token']
                oauth_token_secret = config['oauth_token_secret']
                
                access_token_key, access_token_secret = oauth_finish(
                    consumer_key, consumer_secret,
                    oauth_token, oauth_token_secret,
                    pin)
                    
                config['access_token_key'] = access_token_key
                config['access_token_secret'] = access_token_secret
                source.setup_state = SourceSetupState.ready
                source.config = json.dumps(config)
                core.add(source)
                
                return True, cls(core, source)
            
        else:
            # everything should be fine
            return True, cls(core, source)
    
    @classmethod
    def update(cls, core, source):
        if source.config is None:
            source.config = '{}'
            core.add(source)
        
        if source.version < cls.version:
            # update anything if needed
            
            # if anything was updated, then the db entry should be updated as well
            source.version = cls.version
            core.add(source)
    
    def __init__(self, core, source, config=None):
        self.core = core
        self.source = source
        self.log = core.logger
        self.session = core.session
        
        self.autocommit = False
        
        if config is not None:
            self._load_config(config)
        else:
            self._load_config(json.loads(source.config))
        
        self._init_api()
    
    def _load_config(self, config):
        self.consumer_key = config.get('consumer_key', CONSUMER_KEY)
        self.consumer_secret = config.get('consumer_secret', CONSUMER_SECRET)
        
        self.access_token_key = config.get('access_token_key', None)
        self.access_token_secret = config.get('access_token_secret', None)
        
        self.sub_preview = config.get('sub_preview', True)
    
    def _init_api(self):
        self.api = twitter.Api(
            consumer_key=self.consumer_key,
            consumer_secret=self.consumer_secret,
            access_token_key=self.access_token_key,
            access_token_secret=self.access_token_secret)
        
        credentials = self.api.VerifyCredentials()
        
        self.user = credentials.screen_name
        #self.name = credentials.name
        #self.profile_image = credentials.profile_image_url
    
    def test(self):
        # to be used in the future, might as well implement now (even if it just returns true)
        # this function will be manually called by the user and is used to test if the api works with the current configuration
        # usually, this means making the simplest call to the api to make sure everything is working properly
        # if no problem is found with the config this method should always return True, even if there is a network problem that prevents connectivity
        
        # this method will not be called unless Twitter.init actually returns an instance, so we only need to test the api itself
        # if this function returns False, then setup_state should be set to an appropriate value
        return True
    
    
    def get_url(self, remote_post):
        """
        Gets the url for a RemotePost.
        This function should the stored information to generate the url.
        If the remote_id is not enough, any extra data (or even the url itself),
        should be stored in the metadata field.
        If no url can be generated, then None should be returned.
        """
        
        metadata = json.loads(remote_post.metadata_)
        return TWEET_FORMAT.format(user=metadata.get('user', '_'), tweet_id=remote_post.remote_id)
    
    def can_download(self, url):
        """
        Checks if an url can be downloaded by this plugin.
        
        Returns True if this plugin is able to download the url.
        """
        is_valid_url = bool(TWEET_REGEXP.match(url))
        is_digit = url.isdigit()
        return is_valid_url or is_digit
    
    def _download_file(self, url):
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
    
    def _tweet_to_remote_post(self, tweet, preview=False):
        # get the original tweet if this is a retweet
        if tweet.retweeted_status is not None:
            tweet = tweet.retweeted_status
        
        remote_id = tweet.id_str
        user = tweet.user.screen_name
        text = tweet.full_text
        post_time = datetime.utcfromtimestamp(tweet.created_at_in_seconds)
        
        self.log.info('downloading tweet %s', remote_id)
        
        post = self.session.query(RemotePost).filter(RemotePost.source_id == self.source.id, RemotePost.remote_id == remote_id).one_or_none()
        if post is None:
            self.log.info('creating new post')
            post = RemotePost(
                source=self.source,
                remote_id=remote_id,
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
    
    def download(self, url, preview=False):
        """
        Creates or updates a RemotePost entry along with all the associated Files,
        and downloads all files and thumbnails that aren't present yet.
        
        If preview is set to True, then only the thumbnails are downloaded.
        
        Returns the downloaded RemotePost object.
        """
        
        self.log.info('download request for %s', url)
        if url.isdigit():
            tweet_id = url
            
        else:
            match = TWEET_REGEXP.match(url)
            if not match:
                # TODO throw
                return None
            
            tweet_id = match.group('tweet_id')
        
        tweet = self.api.GetStatus(tweet_id)
        self.log.debug('tweet: %s', tweet)
        
        return self._tweet_to_remote_post(tweet, preview)
    
    _supported_methods = ['tweets', 'retweets', 'likes']
    # TODO define how search actually comes from hoordu
    # for now it's just a string
    def create_subscription(self, name, search):
        """
        Creates a Subscription entry for a given search identified by the given name,
        should not get any posts from the post source.
        """
        
        method, user = search.split(':')
        
        if method not in self._supported_methods:
            # TODO throw
            return None
        
        search_options = {
            'method': method,
            'user': user
        }
        initial_state = {}
        
        sub = Subscription(
            source=self.source,
            name=name,
            options=json.dumps(search_options),
            state=json.dumps(initial_state)
        )
        
        self.core.add(sub)
        self.core.flush()
        
        return sub
    
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
    
    def _iterator_from_subscription(self, subscription, head=True, limit=None):
        options = json.loads(subscription.options)
        state = json.loads(subscription.state)
        
        method = options['method']
        user = options['user']
        
        page_size = PAGE_LIMIT if limit is None else min(limit, PAGE_LIMIT)
        since_id = state.get('head_id') if head else None
        max_id = state.get('tail_id') if not head else None
        
        # max_id: Returns results with an ID less than (that is, older than) or equal to the specified ID.
        if max_id is not None:
            max_id = int(max_id) - 1
        
        if method == 'tweets':
            tweets = self._page_iterator(
                self.api.GetUserTimeline,
                limit=limit,
                screen_name=user, count=page_size, exclude_replies=False, include_rts=False,
                max_id=max_id, since_id=since_id
            )
            
        elif method == 'retweets':
            tweets = self._page_iterator(
                self.api.GetUserTimeline,
                limit=limit,
                screen_name=user, count=page_size, exclude_replies=False, include_rts=True,
                max_id=max_id, since_id=since_id
            )
            
        elif method == 'likes':
            tweets = self._page_iterator(
                self.api.GetFavorites,
                limit=limit,
                screen_name=user, count=page_size,
                max_id=max_id, since_id=since_id
            )
        
        else:
            tweets = []
        
        return tweets
    
    def update_subscription(self, subscription):
        """
        Gets every post in the subscription up until the first post found during the
        last execution of this method, or until no more posts are found if this method
        was never executed.
        Create a RemotePost entry and any associated Files for each post found,
        thumbnails should be downloaded, files are optional.
        
        Returns a list of the new RemotePost objects.
        """
        
        state = json.loads(subscription.state)
        tail_id = state.get('tail_id')
        first_execution = tail_id is None
        
        tweets = self._iterator_from_subscription(subscription, True)
        
        head_id = None
        posts = []
        for tweet in tweets:
            if head_id is None:
                head_id = tweet.id_str
            
            post = self._tweet_to_remote_post(tweet, self.sub_preview)
            posts.append(post)
            subscription.feed.append(post)
            
            if self.autocommit:
                self.core.commit()
            
            tail_id = tweet.id_str
        
        if head_id is not None:
            state['head_id'] = head_id
        
        if first_execution:
            state['tail_id'] = tail_id
        
        subscription.state = json.dumps(state)
        self.core.add(subscription)
        
        return posts
    
    def fetch_subscription(self, subscription, n=20):
        """
        Try to get `n` posts starting from the oldest post found in this subscription.
        Create a RemotePost entry and any associated Files for each post found,
        thumbnails should be downloaded, files are optional.
        
        Returns a list of the new RemotePost objects.
        """
        
        state = json.loads(subscription.state)
        tail_id = state.get('tail_id')
        first_execution = tail_id is None
        
        tweets = self._iterator_from_subscription(subscription, False, limit=n)
        
        head_id = None
        posts = []
        for tweet in tweets:
            if head_id is None and first_execution:
                head_id = tweet.id_str
            
            post = self._tweet_to_remote_post(tweet, self.sub_preview)
            posts.append(post)
            subscription.feed.append(post)
            
            if self.autocommit:
                self.core.commit()
            
            tail_id = tweet.id_str
        
        if head_id is not None:
            state['head_id'] = head_id
        
        state['tail_id'] = tail_id
        subscription.state = json.dumps(state)
        
        self.core.add(subscription)
        
        return posts


Plugin = Twitter
