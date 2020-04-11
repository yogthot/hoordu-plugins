#!/usr/bin/env python3

import sys
from pathlib import Path
import importlib.util
import traceback

import hoordu
from hoordu.models import Service, Subscription

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


# this should be the general approach to initialization of an api
def init(manager, Api, parameters):
    while True:
        # attempt to init
        success, api = Api.init(manager, parameters=parameters)
        
        if success:
            manager.commit()
            return api
        
        elif api is not None:
            # if not successful but something else was returned
            # then attempt to ask the user for input
            
            # TODO simple forms
            prompt = api
            
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
    plugin = load_module('{0}/{0}.py'.format(plugin_name)).plugin
    
    manager = hrd.get_manager(plugin_name)
    api = init(manager, plugin, plugin_config)
    # this property is supposed to make the plugin implementation commit
    # after every post is imported, rather than leaving it to the caller
    # temporary until I find a better way to configure plugins
    api.autocommit = True
    
    try:
        if command == 'download':
            url = args[0]
            
            if api.can_download(url):
                api.download(url, preview=False)
                manager.commit()
            else:
                fail('can\'t download the given url: {0}', url)
            
        elif command == 'sub':
            sub_name = args[0]
            sub_search = args[1]
            
            sub = manager.session.query(Subscription).filter(Subscription.service_id == api.service.id, Subscription.name == sub_name).one_or_none()
            if sub is None:
                print('creating subscription \'{0}\' with search \'{1}\''.format(sub_name, sub_search))
                sub = api.create_subscription(sub_name, sub_search)
                manager.commit()
                
            else:
                fail('subscription named \'{0}\' already exists', sub_name)
            
        elif command == 'update':
            sub_name = args[0]
            
            sub = manager.session.query(Subscription).filter(Subscription.service_id == api.service.id, Subscription.name == sub_name).one_or_none()
            if sub is not None:
                print('getting all new posts for subscription \'{0}\''.format(sub_name))
                api.update_subscription(sub)
                manager.commit()
                
            else:
                fail('subscription named \'{0}\' doesn\'t exist', sub_name)
            
        elif command == 'list':
            subs = manager.session.query(Subscription).filter(Subscription.service_id == api.service.id)
            for sub in subs:
                print('\'{0}\': {1}'.format(sub.name, sub.options))
            
        elif command == 'update-all':
            subs = manager.session.query(Subscription).filter(Subscription.service_id == api.service.id)
            for sub in subs:
                print('getting all new posts for subscription \'{0}\''.format(sub.name))
                api.update_subscription(sub)
                manager.commit()
            
        elif command == 'fetch':
            sub_name = args[0]
            num_posts = int(args[1])
            
            sub = manager.session.query(Subscription).filter(Subscription.service_id == api.service.id, Subscription.name == sub_name).one_or_none()
            if sub is not None:
                print('fetching {0} posts for subscription \'{1}\''.format(num_posts, sub_name))
                api.fetch_subscription(sub, num_posts)
                manager.commit()
                
            else:
                fail('subscription named \'{0}\' doesn\'t exist', sub_name)
        
        elif command == 'unsub':
            sub_name = args[0]
            
            manager.session.query(Subscription).filter(Subscription.service_id == api.service.id, Subscription.name == sub_name).delete()
            manager.commit()
        
    except SystemExit:
        pass
        
    except:
        traceback.print_exc()
        # rollback whatever was being done at the time
        manager.rollback()
        sys.exit(1)


