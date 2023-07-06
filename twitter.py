import os
import re
from datetime import datetime, timezone
from tempfile import mkstemp
import shutil
from urllib.parse import urlparse
import functools
from collections import OrderedDict
import dateutil.parser

import aiohttp
import contextlib
import asyncio

import hoordu
from hoordu.models import *
from hoordu.plugins import *
from hoordu.forms import *

DOMAIN = 'tweetdeck.twitter.com'
TWEET_DETAIL_URL = f'https://{DOMAIN}/i/api/graphql/Pn68XRZwyV9ClrAEmK8rrQ/TweetDetail'
USER_BY_ID = f'https://{DOMAIN}/i/api/graphql/8slyDObmnUzBOCu7kYZj_A/UserByRestId'
USER_BY_SCREENNAME = f'https://{DOMAIN}/i/api/graphql/qRednkZG-rn1P6b48NINmQ/UserByScreenName'
TIMELINE_URL = f'https://{DOMAIN}/i/api/graphql/wxoVeDnl0mP7VLhe6mTOdg/UserTweetsAndReplies'
MEDIATIMELINE_URL = f'https://{DOMAIN}/i/api/graphql/Az0-KW6F-FyYTc2OJmvUhg/UserMedia'
LIKES_URL = f'https://{DOMAIN}/i/api/graphql/kgZtsNyE46T3JaEf2nF9vw/Likes'

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

PAGE_LIMIT = 40

class TwitterClient:
    def __init__(self, csrf, token, auth_token):
        self.csrf = csrf
        self.token = token
        self.auth_token = auth_token
        
        cookies = {
            #'guest_id': 'v1:167122743758400190',
            'ct0': csrf,
            #'kdt': 'hWhELjWzETg53Upq1zXYtf2IA4UxlExxOBPF5wqC',
            #'twid': 'u=912706848',
            'auth_token': auth_token,
            #'d_prefs': 'MjoxLGNvbnNlbnRfdmVyc2lvbjoyLHRleHRfdmVyc2lvbjoxMDAw',
            #'eu_cn': '1',
            #'des_opt_in': 'N',
            #'dnt': '1',
        }
        headers = {
            'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64; rv:109.0) Gecko/20100101 Firefox/113.0',
            'Accept': '*/*',
            'Accept-Language': 'en-US,en;q=0.5',
            'Referer': 'https://twitter.com/',
            'authorization': f'Bearer {token}',
            'x-twitter-auth-type': 'OAuth2Session',
            'x-csrf-token': csrf,
            #'x-twitter-client-language': 'en',
            #'x-twitter-active-user': 'yes',
            'Pragma': 'no-cache',
            'Cache-Control': 'no-cache',
        }
        
        self.http = aiohttp.ClientSession(cookies=cookies, headers=headers)
    
    async def __aenter__(self) -> 'TwitterClient':
        await self.http.__aenter__()
        return self
    
    async def __aexit__(self, *args):
        return await self.http.__aexit__(*args)
    
    @contextlib.asynccontextmanager
    async def _get(self, *args, **kwargs):
        async with self.http.get(*args, **kwargs) as resp:
            yield resp
            return
    
    async def get_user(self, user_id=None, *, username=None):
        variables = {
            'withSafetyModeUserFields': True,
        }
        features = {
            'hidden_profile_likes_enabled': False,
            'responsive_web_graphql_exclude_directive_enabled': True,
            'verified_phone_label_enabled': False,
            'highlights_tweets_tab_ui_enabled': True,
            'creator_subscriptions_tweet_preview_api_enabled': True,
            'responsive_web_graphql_skip_user_profile_image_extensions_enabled': False,
            'responsive_web_graphql_timeline_navigation_enabled': True,
        }
        
        if user_id is not None:
            url = USER_BY_ID
            variables['userId'] = user_id
            
        elif username is not None:
            url = USER_BY_SCREENNAME
            variables['screen_name'] = username
            features['subscriptions_verification_info_verified_since_enabled'] = True
        
        params = {
            'variables': json.dumps(variables),
            'features': json.dumps(features),
        }
        
        async with self._get(url, params=params) as resp:
            body = hoordu.Dynamic.from_json(await resp.text())
        
        user = body.get_path('data', 'user', 'result')
        
        if user is None:
            raise APIError('This account does not exist')
        
        if user['__typename'] == 'UserUnavailable':
            raise APIError(f'{user.reason}: {user.unavailable_message.text}')
        
        return user
    
    async def get_tweet(self, tweet_id):
        #tweet_id = str(tweet_id)
        variables = {
            'focalTweetId': tweet_id,
            'with_rux_injections': False,
            'includePromotedContent': True,
            'withCommunity': True,
            'withQuickPromoteEligibilityTweetFields': True,
            'withBirdwatchNotes': True,
            'withVoice': True,
            'withV2Timeline': True,
        }
        features = {
            'rweb_lists_timeline_redesign_enabled': True,
            'responsive_web_graphql_exclude_directive_enabled': True,
            'verified_phone_label_enabled': False,
            'creator_subscriptions_tweet_preview_api_enabled': True,
            'responsive_web_graphql_timeline_navigation_enabled': True,
            'responsive_web_graphql_skip_user_profile_image_extensions_enabled': False,
            'tweetypie_unmention_optimization_enabled': True,
            'responsive_web_edit_tweet_api_enabled': True,
            'graphql_is_translatable_rweb_tweet_is_translatable_enabled': True,
            'view_counts_everywhere_api_enabled': True,
            'longform_notetweets_consumption_enabled': True,
            'tweet_awards_web_tipping_enabled': False,
            'freedom_of_speech_not_reach_fetch_enabled': True,
            'standardized_nudges_misinfo': True,
            'tweet_with_visibility_results_prefer_gql_limited_actions_policy_enabled': False,
            'longform_notetweets_rich_text_read_enabled': True,
            'longform_notetweets_inline_media_enabled': False,
            'responsive_web_enhance_cards_enabled': False,
        }
        params = {
            'variables': json.dumps(variables),
            'features': json.dumps(features),
        }
        
        async with self._get(TWEET_DETAIL_URL, params=params) as resp:
            body = hoordu.Dynamic.from_json(await resp.text())
        
        instructions = body.data.threaded_conversation_with_injections_v2.instructions
        for inst in instructions:
            if inst.type == 'TimelineAddEntries':
                entries = inst.entries
                for entry in entries:
                    if entry.content.entryType == 'TimelineTimelineItem' \
                            and entry.content.itemContent.itemType == 'TimelineTweet':
                        tweet = entry.content.itemContent.tweet_results.result
                        if 'tweet' in tweet: tweet = tweet.tweet
                        if tweet.rest_id == tweet_id:
                            return tweet
    
    async def get_timeline(self, user_id, count=PAGE_LIMIT, cursor=None):
        variables = {
            'userId': user_id,
            'count': count,
            'includePromotedContent': True,
            'withCommunity': True,
            'withVoice': True,
            'withV2Timeline': True
        }
        features = {
            'rweb_lists_timeline_redesign_enabled': True,
            'responsive_web_graphql_exclude_directive_enabled': True,
            'verified_phone_label_enabled': False,
            'creator_subscriptions_tweet_preview_api_enabled': True,
            'responsive_web_graphql_timeline_navigation_enabled': True,
            'responsive_web_graphql_skip_user_profile_image_extensions_enabled': False,
            'tweetypie_unmention_optimization_enabled': True,
            'responsive_web_edit_tweet_api_enabled': True,
            'graphql_is_translatable_rweb_tweet_is_translatable_enabled': True,
            'view_counts_everywhere_api_enabled': True,
            'longform_notetweets_consumption_enabled': True,
            'responsive_web_twitter_article_tweet_consumption_enabled': False,
            'tweet_awards_web_tipping_enabled': False,
            'freedom_of_speech_not_reach_fetch_enabled': True,
            'standardized_nudges_misinfo': True,
            'tweet_with_visibility_results_prefer_gql_limited_actions_policy_enabled': True,
            'longform_notetweets_rich_text_read_enabled': True,
            'longform_notetweets_inline_media_enabled': True,
            'responsive_web_media_download_video_enabled': False,
            'responsive_web_enhance_cards_enabled': False
        }
        fieldToggles = {
            'withArticleRichContentState': False
        }
        
        if cursor is not None:
            variables['cursor'] = cursor
        
        params = {
            'variables': json.dumps(variables),
            'features': json.dumps(features),
            'fieldToggles': json.dumps(fieldToggles),
        }
        
        async with self._get(TIMELINE_URL, params=params) as resp:
            text = await resp.text()
            try:
                return hoordu.Dynamic.from_json(text)
            except:
                raise ApiError(text)
    
    async def get_media_timeline(self, user_id, count=PAGE_LIMIT, cursor=None):
        variables = {
            'userId': user_id,
            'count': count,
            'includePromotedContent': False,
            'withClientEventToken': False,
            'withBirdwatchNotes': False,
            'withVoice': True,
            'withV2Timeline': True,
        }
        features = {
            'rweb_lists_timeline_redesign_enabled': True,
            'responsive_web_graphql_exclude_directive_enabled': True,
            'verified_phone_label_enabled': False,
            'creator_subscriptions_tweet_preview_api_enabled': True,
            'responsive_web_graphql_timeline_navigation_enabled': True,
            'responsive_web_graphql_skip_user_profile_image_extensions_enabled': False,
            'tweetypie_unmention_optimization_enabled': True,
            'responsive_web_edit_tweet_api_enabled': True,
            'graphql_is_translatable_rweb_tweet_is_translatable_enabled': True,
            'view_counts_everywhere_api_enabled': True,
            'longform_notetweets_consumption_enabled': True,
            'responsive_web_twitter_article_tweet_consumption_enabled': False,
            'tweet_awards_web_tipping_enabled': False,
            'freedom_of_speech_not_reach_fetch_enabled': True,
            'standardized_nudges_misinfo': True,
            'tweet_with_visibility_results_prefer_gql_limited_actions_policy_enabled': True,
            'longform_notetweets_rich_text_read_enabled': True,
            'longform_notetweets_inline_media_enabled': True,
            'responsive_web_media_download_video_enabled': False,
            'responsive_web_enhance_cards_enabled': False,
        }
        fieldToggles = {
            'withArticleRichContentState': False,
        }
        
        if cursor is not None:
            variables['cursor'] = cursor
        
        params = {
            'variables': json.dumps(variables),
            'features': json.dumps(features),
            'fieldToggles': json.dumps(fieldToggles),
        }
        
        async with self._get(MEDIATIMELINE_URL, params=params) as resp:
            text = await resp.text()
            try:
                return hoordu.Dynamic.from_json(text)
            except:
                raise ApiError(text)
    
    async def get_likes(self, user_id, count=PAGE_LIMIT, cursor=None):
        variables = {
            'userId': user_id,
            'count': count,
            'includePromotedContent': False,
            'withClientEventToken': False,
            'withBirdwatchNotes': False,
            'withVoice': True,
            'withV2Timeline': True,
        }
        features = {
            'rweb_lists_timeline_redesign_enabled':True,
            'responsive_web_graphql_exclude_directive_enabled':True,
            'verified_phone_label_enabled':False,
            'creator_subscriptions_tweet_preview_api_enabled':True,
            'responsive_web_graphql_timeline_navigation_enabled':True,
            'responsive_web_graphql_skip_user_profile_image_extensions_enabled':False,
            'tweetypie_unmention_optimization_enabled':True,
            'responsive_web_edit_tweet_api_enabled':True,
            'graphql_is_translatable_rweb_tweet_is_translatable_enabled':True,
            'view_counts_everywhere_api_enabled':True,
            'longform_notetweets_consumption_enabled':True,
            'responsive_web_twitter_article_tweet_consumption_enabled':False,
            'tweet_awards_web_tipping_enabled':False,
            'freedom_of_speech_not_reach_fetch_enabled':True,
            'standardized_nudges_misinfo':True,
            'tweet_with_visibility_results_prefer_gql_limited_actions_policy_enabled':True,
            'longform_notetweets_rich_text_read_enabled':True,
            'longform_notetweets_inline_media_enabled':True,
            'responsive_web_media_download_video_enabled':False,
            'responsive_web_enhance_cards_enabled':False
        }
        fieldToggles = {
            'withArticleRichContentState': False,
        }
        
        if cursor is not None:
            variables['cursor'] = cursor
        
        params = {
            'variables': json.dumps(variables),
            'features': json.dumps(features),
            'fieldToggles': json.dumps(fieldToggles),
        }
        
        async with self._get(LIKES_URL, params=params) as resp:
            return hoordu.Dynamic.from_json(await resp.text())


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
            
            self.options.user_id = user.rest_id
            
            if self.subscription is not None:
                self.subscription.options = self.options.to_json()
                self.session.add(self.subscription)
            
        else:
            user = await self.api.get_user(user_id=self.options.user_id)
            
    
    def reconfigure(self, direction=FetchDirection.newer, num_posts=None):
        if direction == FetchDirection.newer:
            if self.state.tail_id is None:
                direction = FetchDirection.older
        
        num_posts = None
        
        super().reconfigure(direction=direction, num_posts=num_posts)
    
    def _validate_user(self, tweet):
        try:
            tweet_user_id = tweet.core.user_results.result.rest_id
            is_same_user = (tweet_user_id == self.options.user_id)
        except AttributeError:
            self.log.warning(tweet)
            is_same_user = False
        
        if self.options.method == 'likes':
            return True
        
        return is_same_user
    
    async def _feed_iterator(self):
        cursor = None
        count = PAGE_LIMIT
        
        while True:
            self.log.info('getting next page')
            if self.options.method == 'tweets':
                body = await self.api.get_media_timeline(self.options.user_id, count=count, cursor=cursor)
            elif self.options.method == 'retweets':
                body = await self.api.get_timeline(self.options.user_id, count=count, cursor=cursor)
            elif self.options.method == 'likes':
                body = await self.api.get_likes(self.options.user_id, count=count, cursor=cursor)
            
            try:
                instructions = body.data.user.result.timeline_v2.timeline.instructions
            except:
                self.log.warning(body)
                raise
            
            for inst in instructions:
                if inst.type == 'TimelinePinEntry':
                    entry = inst.entry
                    if entry.content.entryType == 'TimelineTimelineItem':
                        tweet = entry.content.itemContent.tweet_results.get('result')
                        if tweet is not None:
                            if 'tweet' in tweet: tweet = tweet.tweet
                            if self._validate_user(tweet):
                                yield True, tweet.rest_id, tweet
                    
                elif inst.type == 'TimelineAddEntries':
                    for entry in inst.entries:
                        entryType = entry.entryId.rsplit('-', 1)[0]
                        if entryType == 'tweet':
                            tweet = entry.content.itemContent.tweet_results.get('result')
                            if tweet is not None:
                                if 'tweet' in tweet: tweet = tweet.tweet
                                if self._validate_user(tweet):
                                    yield False, (entry.sortIndex if self.options.method == 'likes' else tweet.rest_id), tweet
                            
                        elif entryType == 'profile-conversation':
                            for item in entry.content['items']:
                                tweet = content = item.item.itemContent.tweet_results.result
                                if 'tweet' in tweet: tweet = tweet.tweet
                                if self._validate_user(tweet):
                                    yield False, tweet.rest_id, tweet
                            
                        elif entryType == 'cursor-bottom':
                            cursor = entry.content.value
                            
                        elif entryType in ('who-to-follow', 'cursor-top'):
                            # nop
                            pass
                            
                        elif entryType.startswith('promoted-tweet'):
                            pass
                            
                        else:
                            raise NotImplementedError(f'unknown entry type: {entry.entryId}')
                    
                    if len(inst.entries) == 2:
                        # hopefully this is the last page?
                        # only 2 cursor entries...
                        return
                    
                elif inst.type in ('TimelineClearCache', 'TimelineTimelineModule'):
                    # nop
                    pass
                    
                else:
                    raise NotImplementedError(f'unknown instruction type: {inst.type}')
    
    def _validate_method(self, tweet):
        is_retweet = False
        retweeted = tweet.legacy.get('retweeted_status_result')
        if retweeted is not None:
            is_retweet = True
            tweet = retweeted.result
            if 'tweet' in tweet: tweet = tweet.tweet
        
        media_list = tweet.legacy.get_path('extended_entities', 'media')
        urls = tweet.legacy.get_path('entities', 'urls')
        
        has_media = ((
            media_list is not None and
            len(media_list) > 0
        ) or (
            urls is not None and
            len(urls) > 0
        ))
        
        if self.options.method == 'retweets':
            return has_media and is_retweet
            
        elif self.options.method == 'tweets':
            return has_media and not is_retweet
            
        elif self.options.method == 'likes':
            return has_media
    
    async def generator(self):
        is_first_tweet = True
        
        async for is_pinned, sort_index, tweet in self._feed_iterator():
            if tweet.get('__typename') == 'TweetTombstone':
                # sometimes deleted tweets show up
                continue
            
            if not is_pinned and is_first_tweet:
                if self.state.head_id is None or self.direction == FetchDirection.newer:
                    self.first_id = sort_index
                
                is_first_tweet = False
            
            if not is_pinned and self.direction == FetchDirection.newer and int(sort_index) <= int(self.state.head_id):
                break
            
            if self._validate_method(tweet):
                remote_post = await self.plugin._to_remote_post(tweet, preview=self.subscription is None)
                yield remote_post
                
                if self.subscription is not None:
                    await self.subscription.add_post(remote_post)
                
                await self.session.commit()
            
            if not is_pinned and self.direction == FetchDirection.older:
                self.state.tail_id = sort_index
        
        if self.first_id is not None:
            self.state.head_id = self.first_id
            self.first_id = None
        
        if self.subscription is not None:
            self.subscription.state = self.state.to_json()
            self.session.add(self.subscription)
        
        await self.session.commit()


class Twitter(SimplePlugin):
    name = 'twitter'
    version = 5
    iterator = TweetIterator
    
    @classmethod
    def config_form(cls):
        return Form('{} config'.format(cls.name),
            ('csrf', Input('csrf', [validators.required])),
            ('bearer_token', Input('bearer token', [validators.required])),
            ('auth_token', Input('auth token', [validators.required])),
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
        
        if not config.contains('csrf', 'bearer_token', 'auth_token'):
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
            async with TwitterClient(self.config.csrf, self.config.bearer_token, self.config.auth_token) as api:
                self.api: TwitterClient = api
                yield self
    
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
        variants = media.get_path('video_info', 'variants')
        if variants is None:
            variants = []
        
        variant = max(
            [v for v in variants if 'bitrate' in v],
            key=lambda v: v['bitrate'],
            default=None
        )
        
        if variant is not None:
            path, _ = await self.session.download(variant['url'])
            return path
        else:
            return None
    
    async def _to_remote_post(self, tweet, remote_post=None, preview=False):
        # get the original tweet if this is a retweet
        retweeted = tweet.legacy.get('retweeted_status_result')
        if retweeted is not None:
            tweet = retweeted.result
            if 'tweet' in tweet: tweet = tweet.tweet
        
        author = tweet.core.user_results.result
        
        original_id = tweet.rest_id
        user = author.legacy.screen_name
        user_id = author.rest_id
        text = tweet.legacy.full_text
        post_time = dateutil.parser.parse(tweet.legacy.created_at).astimezone(timezone.utc).replace(tzinfo=None)
        
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
        
        if tweet.legacy.possibly_sensitive:
            nsfw_tag = await self._get_tag(TagCategory.meta, 'nsfw')
            await remote_post.add_tag(nsfw_tag)
        
        hashtags = tweet.legacy.get_path('entities', 'hashtags')
        if hashtags is not None:
            for hashtag in hashtags:
                tag = await self._get_tag(TagCategory.general, hashtag.text)
                await remote_post.add_tag(tag)
        
        async def add_related_tweet(related):
            if 'tweet' in related: related = related.tweet
            if related.get('__typename') == 'TweetTombstone':
                return
            
            related_id = related.rest_id
            related_user = related.core.user_results.result
            url = TWEET_FORMAT.format(user=related_user.legacy.screen_name, tweet_id=related_id)
            await remote_post.add_related_url(url)
        
        # no easy way to get this now
        #await add_related_tweet('replied_to')
        
        quoted = tweet.get_path('quoted_status_result', 'result')
        if quoted is not None:
            await add_related_tweet(quoted)
        
        urls = tweet.legacy.get_path('entities', 'urls')
        if urls is not None:
            for url in urls:
                # hopefully no t.co bullshit here
                await remote_post.add_related_url(url.expanded_url)
        
        self.session.add(remote_post)
        
        media_list = tweet.legacy.get_path('extended_entities', 'media')
        if media_list is not None:
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
                        base_url, ext = media.media_url_https.rsplit('.', 1)
                        filename = '{}.{}'.format(base_url.rsplit('/', 1)[-1], ext)
                        
                        if need_thumb:
                            thumb = await self._download_media_file(base_url, ext, THUMB_SIZE, filename)
                        
                        if need_file:
                            orig = await self._download_media_file(base_url, ext, ORIG_SIZE, filename)
                        
                        await self.session.import_file(file, orig=orig, thumb=thumb, move=True)
                        file.ext = ext
                        file.thumb_ext = ext
                        self.session.add(file)
                        
                    elif media.type in ('video', 'animated_gif'):
                        base_url, ext = media.media_url_https.rsplit('.', 1)
                        filename = '{}.{}'.format(base_url.rsplit('/', 1)[-1], ext)
                        
                        if need_thumb:
                            thumb = await self._download_media_file(base_url, ext, THUMB_SIZE, filename)
                        
                        if need_file:
                            orig = await self._download_video(media)
                        
                        await self.session.import_file(file, orig=orig, thumb=thumb, move=True)
                        file.thumb_ext = ext
                        self.session.add(file)
                        
                    else:
                        raise NotImplementedError('unknown media type: {}'.format(media.type))
        
        return remote_post
    
    async def download(self, id=None, remote_post=None, preview=False):
        if id is None and remote_post is None:
            raise ValueError('either id or remote_post must be passed')
        
        if remote_post is not None:
            id = remote_post.original_id
        
        tweet = await self.api.get_tweet(id)
        
        return await self._to_remote_post(tweet, remote_post=remote_post, preview=preview)
    
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
        
        user = await self.api.get_user(**kwargs)
        options.user_id = user.rest_id
        
        urls = user.legacy.get_path('entities', 'url', 'urls') or []
        description_urls = user.legacy.get_path('entities', 'description', 'urls') or []
        related_urls = {u.expanded_url for u in urls + description_urls}
        
        thumb_url = user.legacy.profile_image_url_https
        match = PROFILE_IMAGE_REGEXP.match(thumb_url)
        if match:
            thumb_url = match.group('base') + PROFILE_THUMB_SIZE + match.group('ext')
        
        return SearchDetails(
            hint=user.legacy.screen_name,
            title=user.legacy.name,
            description=user.legacy.description,
            thumbnail_url=thumb_url,
            related_urls=related_urls
        )

Plugin = Twitter


