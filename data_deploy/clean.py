import concurrent.futures
import os

import remoto

import data_deploy.internal.defaults.deploy as defaults
from data_deploy.internal.platform.registrar import Registrar
from data_deploy.internal.platform.platform import register_plugins
from data_deploy.internal.remoto.ssh_wrapper import get_wrapper, get_wrappers, close_wrappers
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


def _merge_kwargs(x, y):
    z = x.copy()
    z.update(y)
    return z


def _clean_internal(connectionwrappers, paths, sudo):
    with concurrent.futures.ThreadPoolExecutor(max_workers=len(connectionwrappers)) as executor:
        futures_rm = [executor.submit(remoto.process.check, connectionwrapper.connection, '{}rm -rf {}'.format('sudo ' if sudo else '', path), shell=True) for connectionwrapper in connectionwrappers.values() for path in paths]
        return all(x.result()[2] == 0 for x in futures_rm)


def clean(reservation, key_path=None, connectionwrappers=None, paths=[], sudo=False, silent=False):
    '''Deploy data using the CLI. Loads plugin with given `plugin` name, parses args, executes.
    Args:
        key_path (optional str): If set, uses given key to connect to remote nodes.
        connectionwrappers (optional dict(metareserve.Node, RemotoSSHWrapper)): If set, uses given connections, instead of building new ones.
        paths (optional list): Remote data paths to remove.
        sudo (optional bool): If set, uses sudo to clean remote paths.
        silent (optional bool): If set, does not print so much.

    Returns:
        `True` on success, `False` on failure.'''
    local_connections = connectionwrappers == None
    if local_connections:
        ssh_kwargs = {'IdentitiesOnly': 'yes', 'StrictHostKeyChecking': 'no'}
        if key_path:
            ssh_kwargs['IdentityFile'] = key_path
        connectionwrappers = get_wrappers(reservation.nodes, lambda node: node.ip_public, ssh_params=lambda node: _merge_kwargs(ssh_kwargs, {'User': node.extra_info['user']}), silent=silent)
    else:
        if not all(x.open for x in connectionwrappers):
            raise ValueError('SSH installation failed: At least one connection is already closed.')

    if (not paths) or not any(paths):
        printw('No paths to clean provided.')
        if local_connections:
            close_wrappers(connectionwrappers)
        return True

    retval = _clean_internal(connectionwrappers, [_clean_dest(x) for x in paths], sudo)
    if local_connections:
        close_wrappers(connectionwrappers)
    if not silent:
        if retval:
            prints('Cleaned specified remote paths.')
        else:
            printe('Could not clean all specified remote paths')

    return retval