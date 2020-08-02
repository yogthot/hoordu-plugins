#!/usr/bin/env python3

import sys
from pathlib import Path
import importlib.util
import traceback

import hoordu
from hoordu.models import Source, Subscription
from hoordu.plugins import FetchDirection

def load_module(filename):
    module_name = Path(filename).name.split('.')[0]
    spec = importlib.util.spec_from_file_location(module_name, filename)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module

def usage():
    print('python3 {0} <plugin> <command> [command arguments]'.format(sys.argv[0]))
    print('')
    print('available commands:')
    print('    download <url>')
    print('        attempts to download the given url')
    print('')
    print('    sub <sub_name> <sub_search>')
    print('        creates a subscription with the given name and search')
    print('')
    print('    update <sub_name>')
    print('        gets all new posts for a subscription')
    print('')
    print('    fetch <sub_name> <n>')
    print('        gets <n> old posts from a subscription')

def fail(format, *args, **kwargs):
    print(format.format(*args, **kwargs))
    sys.exit(1)


# this should be the general approach to initialization of a plugin
def init(hrd, Plugin, parameters):
    while True:
        # attempt to init
        success, plugin = hrd.init_plugin(Plugin, parameters=parameters)
        
        if success:
            hrd.core.commit()
            return plugin
        
        elif plugin is not None:
            # if not successful but something else was returned
            # then attempt to ask the user for input
            
            # TODO simple forms
            prompt = plugin
            
            user_input = input('{}: '.format(prompt))
            
            # update parameters and try again
            parameters = {'user_input': user_input}
        
        else:
            print('something went wrong with the authentication')
            sys.exit(1)

if __name__ == '__main__':
    if len(sys.argv) < 3:
        usage()
        sys.exit(1)
    
    plugin_name = sys.argv[1]
    command = sys.argv[2]
    args = sys.argv[3:]
    
    config = hoordu.load_config('hoordu.conf')
    hrd = hoordu.hoordu(config)
    
    plugin_config = hoordu.load_config('{0}/{0}.conf'.format(plugin_name))
    Plugin = load_module('{0}/{0}.py'.format(plugin_name)).Plugin
    
    plugin = init(hrd, Plugin, plugin_config)
    
    core = plugin.core
    
    try:
        if command == 'download':
            url = args[0]
            
            if plugin.can_download(url):
                remote_post = plugin.download(url, preview=False)
                core.commit()
                
                print('related urls:')
                for related in remote_post.related:
                    print('    {}'.format(related.url))
            else:
                fail('can\'t download the given url: {0}', url)
            
        elif command == 'sub':
            sub_name = args[0]
            sub_search = args[1]
            
            sub = core.session.query(Subscription).filter(Subscription.source_id == plugin.source.id, Subscription.name == sub_name).one_or_none()
            if sub is None:
                print('creating subscription \'{0}\' with search \'{1}\''.format(sub_name, sub_search))
                sub = plugin.create_subscription(sub_name, sub_search)
                core.commit()
                
            else:
                fail('subscription named \'{0}\' already exists', sub_name)
            
        elif command == 'update':
            sub_name = args[0]
            
            sub = core.session.query(Subscription).filter(Subscription.source_id == plugin.source.id, Subscription.name == sub_name).one_or_none()
            if sub is not None:
                print('getting all new posts for subscription \'{0}\''.format(sub_name))
                it = plugin.get_iterator(sub)
                it.fetch(direction=FetchDirection.newer, n=None)
                core.commit()
                
            else:
                fail('subscription named \'{0}\' doesn\'t exist', sub_name)
            
        elif command == 'list':
            subs = core.session.query(Subscription).filter(Subscription.source_id == plugin.source.id)
            for sub in subs:
                print('\'{0}\': {1}'.format(sub.name, sub.options))
            
        elif command == 'update-all':
            subs = core.session.query(Subscription).filter(Subscription.source_id == plugin.source.id)
            for sub in subs:
                try:
                    print('getting all new posts for subscription \'{0}\''.format(sub.name))
                    it = plugin.get_iterator(sub)
                    it.fetch(direction=FetchDirection.newer, n=None)
                    core.commit()
                except:
                    traceback.print_exc()
            
        elif command == 'fetch':
            sub_name = args[0]
            num_posts = int(args[1])
            
            sub = core.session.query(Subscription).filter(Subscription.source_id == plugin.source.id, Subscription.name == sub_name).one_or_none()
            if sub is not None:
                print('fetching {0} posts for subscription \'{1}\''.format(num_posts, sub_name))
                it = plugin.get_iterator(sub)
                it.fetch(direction=FetchDirection.older, n=num_posts)
                core.commit()
                
            else:
                fail('subscription named \'{0}\' doesn\'t exist', sub_name)
        
        elif command == 'unsub':
            sub_name = args[0]
            
            core.session.query(Subscription).filter(Subscription.source_id == plugin.source.id, Subscription.name == sub_name).delete()
            core.commit()
        
    except SystemExit:
        pass
        
    except:
        traceback.print_exc()
        # rollback whatever was being done at the time
        core.rollback()
        sys.exit(1)


