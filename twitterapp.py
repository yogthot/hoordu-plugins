from .twitter import Twitter
import twitter

import hoordu
from hoordu.models import *
from hoordu.forms import *

class TwitterNoAuth(Twitter):
    @classmethod
    def config_form(cls):
        return Form('{} config'.format(cls.name),
            ('consumer_key', Input('consumer key', [validators.required])),
            ('consumer_secret', Input('consumer secret', [validators.required]))
        )
    
    @classmethod
    def setup(cls, session, parameters=None):
        plugin = cls.get_plugin(session)
        
        # check if everything is ready to use
        config = hoordu.Dynamic.from_json(plugin.config)
        
        if not config.defined('consumer_key', 'consumer_secret'):
            # try to get the values from the parameters
            if parameters is not None:
                config.update(parameters)
                
                plugin.config = config.to_json()
                session.add(plugin)
        
        if not config.defined('consumer_key', 'consumer_secret'):
            # but if they're still None, the api can't be used
            return False, cls.config_form()
            
        else:
            # the config contains every required property
            return True, None
    
    def _init_api(self):
        self.api = twitter.Api(
            consumer_key=self.config.consumer_key,
            consumer_secret=self.config.consumer_secret,
            application_only_auth=True,
            tweet_mode='extended'
        )

Plugin = TwitterNoAuth

