import os


def root():
    return os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

def implementation_dir():
    '''Path to basic deployment implementation directory.'''
    return os.path.join(root(), 'data_deploy', 'impl')

def ud_plugin_dir():
    '''Path to the user-defined plugin directory.'''
    return os.path.join(os.path.expanduser('~'), '.data-deploy')