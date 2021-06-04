import os

import data_deploy.cli.util as _cli_util
import data_deploy.internal.defaults.deploy as defaults
from data_deploy.internal.platform.registrar import Registrar
from data_deploy.internal.platform.platform import register_plugins
from data_deploy.internal.util.printer import *


def _clean_dest(dest):
    if not dest:
        raise ValueError('Destination not set.')
    dest = os.path.normpath(dest)
    if dest == '~':
        raise ValueError('Sorry, the destination path cannot be the remote home directory. The reason for that is: We use rsync under the hood, and rsync tends to screw up the homedir folder permission if the destination is the homedir.')
    if dest.startswith('~'):
        dest = '/'.join(dest.split('/')[1:])
    if dest.startswith('~'):
        raise ValueError(dest)
    return dest


def deploy_cli(key_path=None, paths=[], dest=defaults.remote_dir(), silent=False, copy_multiplier=1, link_multiplier=1, plugin=None, args=None):
    '''Deploy data using the CLI. Loads plugin with given `plugin` name, parses args, executes.
    Args:
        key_path (optional str): If set, uses given key to connect to remote nodes.
        paths (optional list): Data sources to transport to remote nodes.
        silent (optional bool): If set, does not print so much.
        copy_multiplier (optional int): If set to a value X, makes the dataset X times larger by adding X-1 copies for every file. Applied first.
        link_multiplier (optional int): If set to a value X, makes the dataset X times larger by adding X-1 hardlinks for every file. Applied second.
        plugin (optinal str): Plugin name to load.
        args (optional list(str)): Arguments to parse with plugin.

    Returns:
        `True` on success, `False` on failure.'''
    registrar = Registrar()
    register_plugins(registrar)
    if not silent:
        print('Found {} plugins.'.format(len(registrar)))

    if not plugin in registrar.names:
        printe('Could not find a plugin named "{}"'.format(plugin))
        return False
    plugin = registrar.get(plugin)
    print('Plugin fetched: {}'.format(plugin.path))
    state, args, kwargs = plugin.parse(args)
    if not state:
        printe('Could not parse provided arguments: {}'.format(args))
        return False
    reservation = _cli_util.read_reservation_cli()
    if not reservation:
        return False
    if (not paths) or not any(paths):
        printw('No paths to data given.')
        return False
    dest = _clean_dest(dest)
    return plugin.execute(reservation, key_path, paths, dest, silent, copy_multiplier, link_multiplier, *args, **kwargs)


def deploy(reservation, key_path=None, paths=[], dest=defaults.remote_dir(), silent=False, copy_multiplier=1, link_multiplier=1, plugin=None, *args, **kwargs):
    '''Deploy data. Loads plugin with given `plugin` name, executes.
    Args:
        key_path (optional str): If set, uses given key to connect to remote nodes.
        paths (optional list): Data sources to transport to remote nodes.
        silent (optional bool): If set, does not print so much.
        copy_multiplier (optional int): If set to a value X, makes the dataset X times larger by adding X-1 copies for every file. Applied first.
        link_multiplier (optional int): If set to a value X, makes the dataset X times larger by adding X-1 hardlinks for every file. Applied second.
        plugin (optinal str): Plugin name to load.
        args (optional list(str)): Arguments to pass to plugin.
        kwargs (optional dict(str, any)): Keyword arguments to pass to plugin.

    Returns:
        `True` on success, `False` on failure.'''
    if plugin == None:
        raise ValueError('Caller must specify plugin to use to deploy data.')
    registrar = Registrar()
    register_plugins(registrar)
    print('Found {} plugins.'.format(len(registrar)))
    
    if not plugin in registrar.names:
        printe('Could not find a plugin named "{}"'.format(plugin))
        return False

    if (not paths) or not any(paths):
        printw('No paths to data given.')
        return False
    dest = _clean_dest(dest)
    print('Cleaned dest: {}'.format(dest))
    plugin = registrar.get(plugin)
    return plugin.execute(reservation, key_path, paths, dest, silent, copy_multiplier, link_multiplier, *args, **kwargs)