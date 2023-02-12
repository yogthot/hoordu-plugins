from hoordu.session import HoorduSession
from .twitter import Twitter

import base64

import hoordu
from hoordu.models import *
from hoordu.forms import *

class TwitterNoAuth(Twitter):
    @classmethod
    def config_form(cls):
        return Form('{} config'.format(cls.name),
            ('client_id', Input('client id', [validators.required])),
            ('client_secret', Input('client secret', [validators.required])),
            ('access_token', Input('access token')),
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
            
        elif not config.contains('access_token'):
            config.access_token = await cls._generate_token(session, config.client_id, config.client_secret)
            plugin.config = config.to_json()
            session.add(plugin)
            
            return True, None
            
        else:
            # the config contains every required property
            return True, None
    
    @classmethod
    async def _generate_token(cls, session: HoorduSession, client_id, client_secret):
        # welp gotta try to understand why this is broke
        auth = base64.b64encode(f'{client_id}:{client_secret}'.encode('ascii')).decode('ascii')
        headers = {
            'Content-Type': 'application/x-www-form-urlencoded;charset=UTF-8',
            'Authorization': f'Basic {auth}',
        }
        
        resp = await session.requests.request('https://api.twitter.com/oauth2/token',
            method='POST',
            headers=headers,
            data={'grant_type': 'client_credentials'},
        )
        data = resp.data.decode()
        if resp.status_code != 200:
            raise Exception(data)
        
        return hoordu.Dynamic(data).access_token
    
    async def _refresh_token(self):
        session = self.session.priority
        plugin = await self.get_plugin(session)
        config = hoordu.Dynamic.from_json(plugin.config)
        
        self.log.info('attempting to refresh access token')
        access_token = await self._generate_token(self.session, config.client_id, config.client_secret)
        
        self.config.access_token = access_token
        
        # update access_token in the database
        config.access_token = access_token
        plugin.config = config.to_json()
        session.add(plugin)
        await session.commit()
        
        return access_token

Plugin = TwitterNoAuth

