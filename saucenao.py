
import requests

import hoordu
from hoordu.models import *
from hoordu.plugins import *
from hoordu.forms import *


class SauceNao(ReverseSearchPluginBase):
    name = 'saucenao'
    version = 1
    
    @classmethod
    def config_form(cls):
        return Form('{} config'.format(cls.name),
            ('api_key', Input('api key', [validators.required]))
        )
    
    @classmethod
    def setup(cls, session, parameters=None):
        plugin = cls.get_plugin(session)
        
        config = hoordu.Dynamic.from_json(plugin.config)
        if not config.contains('api_key'):
            if parameters is not None:
                config.update(parameters)
                plugin.config = config.to_json()
                session.add(plugin)
        
        if not config.contains('api_key'):
            return False, cls.config_form()
            
        else:
            return True, None
    
    @classmethod
    def update(cls, session):
        plugin = cls.get_plugin(session)
        
        if plugin.version < cls.version:
            # update anything if needed
            
            # if anything was updated, then the db entry should be updated as well
            plugin.version = cls.version
            session.add(plugin)
    
    def __init__(self, session):
        super().__init__(session)
        
        self.http = requests.Session()
    
    @staticmethod
    def _get_title(data):
        keys = ['title', 'eng_name', 'material', 'source', 'created_at']
        return next(data[k] for k in keys if k in data)
    
    def reverse_search(self, path=None, url=None):
        query = {
            'db': '999',
            'output_type': 2,
            'api_key': self.config.api_key,
            'numres': 4
        }
        if url is not None:
            query['url'] = url
            response = self.http.post('http://saucenao.com/search.php', params=query)
            
        else:
            with open(file, 'r') as f:
                response = self.http.post('http://saucenao.com/search.php', params=query, files={'image.png': f})
        
        json = hoordu.Dynamic.from_json(response.text)
        results = sorted(json.results, key=lambda x: float(x.header.similarity), reverse=True)
        
        for r in results:
            title = self._get_title(r.data)
            
            urls = []
            for url in r.data.get('ext_urls', []):
                urls.append(url)
            
            src = r.data.get('source')
            if src:
                urls.append(src)
            
            yield self._make_result(title, r.header.thumbnail, urls)

Plugin = SauceNao

