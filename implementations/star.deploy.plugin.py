import argparse
import concurrent.futures
import subprocess

import data_deploy.internal.defaults.deploy as defaults
import data_deploy.internal.remoto.ssh_wrapper as ssh_wrapper
import data_deploy.internal.util.fs as fs
import data_deploy.internal.util.location as loc
from data_deploy.internal.util.printer import *


'''Deploys data by sending all data from the local machine to all remotes in parallel.
Works well if bandwidth and local->remote connections don't bottleneck.'''

def _merge_kwargs(x, y):
    z = x.copy()
    z.update(y)
    return z


def _deploy_internal(wrappers, reservation, key_path, paths, dest, strategy, silent, retries):
    if not silent:
        print('Transferring data...')

    with concurrent.futures.ThreadPoolExecutor(max_workers=len(wrappers)) as executor:
        futures_mkdir = [executor.submit(remoto.process.check, x.connection, 'mkdir -p {}'.format(dest), shell=True) for x in wrappers.values()]
        if not all(x.result()[2] == 0 for x in futures_mkdir):
            printe('Could not create data destination directory for all nodes.')
            return False

        fun = lambda path, node, wrapper: subprocess.call('rsync -e "ssh -F {}" -q -aHAX --inplace {} {}:{}'.format(wrapper.ssh_config_path, path, node.ip_public, fs.join(dest, fs.basename(path))), shell=True) == 0
        futures_rsync = [(executor.submit(fun, path, node, wrapper) for node, wrapper in wrappers.items()) for path in paths]
        if not all(x.result() for x in futures_rsync):
            printe('Could not tranfer data to some nodes.')
            return False


def description():
    return "Deploys data by sending all data from the local machine to all remotes in parallel. Works well if bandwidth and local->remote connections don't bottleneck."


def parse(args):
    parser = argparse.ArgumentParser(prog='...')
    # We have no extra arguments to add here.
    args = parser.parse_args(args)
    return True, [], {}


def execute(reservation, key_path, paths, dest, silent, *args, **kwargs):
    connectionwrappers = kwargs.get('connectionwrappers')

    use_local_connections = connectionwrappers == None
    if use_local_connections: # We did not get any connections, so we must make them
        ssh_kwargs = {'IdentitiesOnly': 'yes', 'StrictHostKeyChecking': 'no'}
        if key_path:
            ssh_kwargs['IdentityFile'] = key_path

        connectionwrappers = ssh_wrapper.get_wrappers(reservation.nodes, lambda node: node.ip_public, ssh_params=lambda node: _merge_kwargs(ssh_kwargs, {'User': x.extra_info['user']}), parallel=True, silent=silent)
    else: # We received connections, need to check if they are valid.
        if len(connectionwrappers) != len(reservation):
            raise ValueError('Provided connections do not contain all nodes: reservation length={}, connections amount={}'.format(len(connectionwrappers), len(reservation)))
        if not all(x.open for x in connectionwrappers):
            raise ValueError('Some provided connections are closed.')

    retval = _deploy_internal(connectionwrappers, reservation, key_path, paths, dest, strategy, silent, retries)
    if not use_local_connections:
        _close_connections(connectionwrappers)
    return retval