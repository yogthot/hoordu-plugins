
import requests
from bs4 import BeautifulSoup
from collections import OrderedDict

import hoordu
from hoordu.models import *
from hoordu.plugins import *
from hoordu.forms import *


class Ascii2D(ReverseSearchPluginBase):
    name = 'ascii2d'
    version = 1
    
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
    
    def _parse_generic(self, entry):
        service = entry.select_one('img')['alt'].lower()
        if service == 'twitter':
            link = entry.select_one('a:nth-of-type(1)')
            user_link = entry.select_one('a:nth-of-type(2)')
            return (f'{link.text} - {user_link.text}', link['href'])
            
        else:
            link = entry.select_one('a:nth-of-type(1)')
            return (link.text, link['href'])
    
    def _parse_details(self, det):
        entries = det.select('h6')
        external = det.select_one('.external')
        if len(entries) > 0:
            res = []
            for entry in entries:
                img = entry.select_one('img')
                small = entry.select('small.text-muted')
                
                if img is not None:
                    # pixiv/twitter/nijie/etc
                    res.append(self._parse_generic(entry))
                    
                elif len(small) > 0:
                    # dmm/dlsite
                    title = next(entry.children).strip()
                    for s in small:
                        res.append((title, s.select_one('a')['href']))
            
            return res
            
        elif external is not None:
            # user-added links
            img = external.select_one('img')
            a = external.select('a')
            
            if img is not None:
                # pixiv/twitter/nijie/etc
                return [self._parse_generic(external)]
                
            elif len(a) > 0:
                txt = next(external.children)
                if isinstance(txt, str):
                    title = txt.strip()
                    if title is not None:
                        # dmm/dlsite
                        return [(title, x['href']) for x in a]
                
                else:
                    # recognized url
                    return [(x.text, x['href']) for x in a]
                
            else:
                # unrecognized url, or just text
                text = external.text
                return [(text, text)]
            
        else:
            # probably the first result (empty)
            return []
    
    def reverse_search(self, path=None, url=None):
        index = self.http.get('https://ascii2d.net/')
        index_html = BeautifulSoup(index.text, 'lxml')
        
        csrf_param = index_html.select('meta[name="csrf-param"]')[0]['content']
        csrf_token = index_html.select('meta[name="csrf-token"]')[0]['content']
        
        data = {
            csrf_param: csrf_token
        }
        
        if url:
            data['uri'] = url
            response = self.http.post('https://ascii2d.net/search/uri', data=data)
            
        else:
            with open(path, 'r') as file:
                response = self.http.post('https://ascii2d.net/search/file', data=data, files={'file': file})
        
        html = BeautifulSoup(response.text, 'lxml')
        items = html.select('.item-box')
        
        # parse and group results
        results = OrderedDict()
        entries = []
        for i in items:
            thumb_uri = i.select_one('.image-box img')['src']
            thumb_url = f'https://ascii2d.net{thumb_uri}'
            
            details = i.select_one('.detail-box')
            links = self._parse_details(details)
            
            if len(links) > 0:
                existing = next((u for _, u in links if u in results), None)
                
                if existing is None:
                    title = links[0][0]
                    entry = (thumb_url, title, [u for _, u in links])
                    entries.append(entry)
                    
                    for _, url in links:
                        if url is not None:
                            results[url] = entry
                    
                else:
                    entry = results[existing]
                    
                    for _, url in links:
                        if url not in results:
                            entry[2].append(url)
                            if url is not None:
                                results[url] = entry
        
        for thumb_url, title, sources in entries:
            yield self._make_result(title, thumb_url, sources)

Plugin = Ascii2D

