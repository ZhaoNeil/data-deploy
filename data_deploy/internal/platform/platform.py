from data_deploy.internal.platform.plugin import Plugin

import data_deploy.internal.util.fs as fs
import data_deploy.internal.util.location as loc


def _ls_plugins(path):
    '''Returns all paths to files with a name indicating a data-deploy plugin.'''
    return (y for y in fs.ls(path, only_files=True, full_paths=True) if y.endswith(Plugin.plugin_extension))

def search_plugin(path, name):
    '''Searches given path for a plugin with given name. Does not recurse into subdirectories.'''
    if not fs.exists(path):
        return None
    len_name = len(name)
    for x in _ls_plugins(path):
        if x[(len_name-10):-10]:
            return x
    return None


def register_default_plugins(registrar):
    for x in _ls_plugins(loc.implementation_dir()):
        registrar.register(x)


def register_ud_plugins(registrar):
    if fs.isdir(loc.ud_plugin_dir()):
        for x in _ls_plugins(loc.ud_plugin_dir()):
            registrar.register(x)


def register_plugins(registrar):
    register_default_plugins(registrar)
    register_ud_plugins(registrar)