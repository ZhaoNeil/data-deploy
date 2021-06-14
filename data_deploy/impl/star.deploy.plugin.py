import argparse
import concurrent.futures
import subprocess

import remoto


import data_deploy.shared.copy
import data_deploy.shared.link
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


def _execute_internal(wrappers, reservation, key_path, paths, dest, silent, copy_multiplier, link_multiplier):
    if not silent:
        print('Transferring data...')

    with concurrent.futures.ThreadPoolExecutor(max_workers=len(wrappers)) as executor:
        futures_mkdir = [executor.submit(remoto.process.check, x.connection, 'mkdir -p {}'.format(dest), shell=True) for x in wrappers.values()]
        if not all(x.result()[2] == 0 for x in futures_mkdir):
            printe('Could not create data destination directory for all nodes.')
            return False
        futures_rm = [executor.submit(remoto.process.check, x.connection, 'rm -rf {}/*'.format(dest), shell=True) for x in wrappers.values()]
        if not all(x.result()[2] == 0 for x in futures_rm):
            printe('Could not remove old data from destination directory for all nodes.')
            return False

        fun = lambda path, node, wrapper: subprocess.call('rsync -e "ssh -F {}" -q -aHAXL --inplace {} {}:{}'.format(wrapper.ssh_config_path, path, node.ip_public, fs.join(dest, fs.basename(path))), shell=True) == 0
        futures_rsync = []
        for path in paths:
            futures_rsync += [executor.submit(fun, path, node, wrapper) for node, wrapper in wrappers.items()]
        if not all(x.result() for x in futures_rsync):
            printe('Could not tranfer data to some nodes.')
            return False

        paths_remote = [fs.join(dest, fs.basename(path)) for path in paths]

        copies_amount = max(1, copy_multiplier) - 1
        links_amount = max(1, link_multiplier) - 1
        if copies_amount > 0:
            futures_copy = [executor.submit(data_deploy.shared.copy.copy_single, connection, path, copies_amount, silent=False) for connection in wrappers.values() for path in paths_remote]
            if not all(x.result() for x in futures_copy):
                return False

        if links_amount > 0:
            futures_link = [executor.submit(data_deploy.shared.link.link, connection, expression=data_deploy.shared.copy.copy_expression(dest_file, copies_amount), num_links=links_amount, silent=False) for connection in wrappers.values() for path in paths_remote]
            if not all(x.result() for x in futures_link):
                return False
    return True


def description():
    return "Deploys data by sending all data from the local machine to all remotes in parallel. Works well if bandwidth and local->remote connections don't bottleneck."


def origin():
    return "Default implementation."


def parse(args):
    parser = argparse.ArgumentParser(prog='...')
    # We have no extra arguments to add here.
    args = parser.parse_args(args)
    return True, [], {}


def execute(reservation, key_path, paths, dest, silent, copy_multiplier, link_multiplier, *args, **kwargs):
    connectionwrappers = kwargs.get('connectionwrappers')

    use_local_connections = connectionwrappers == None
    if use_local_connections: # We did not get any connections, so we must make them
        ssh_kwargs = {'IdentitiesOnly': 'yes', 'StrictHostKeyChecking': 'no'}
        if key_path:
            ssh_kwargs['IdentityFile'] = key_path
        connectionwrappers = ssh_wrapper.get_wrappers(reservation.nodes, lambda node: node.ip_public, ssh_params=lambda node: _merge_kwargs(ssh_kwargs, {'User': node.extra_info['user']}), parallel=True, silent=silent)
    else: # We received connections, need to check if they are valid.
        if len(connectionwrappers) != len(reservation):
            raise ValueError('Provided connections do not contain all nodes: reservation length={}, connections amount={}'.format(len(connectionwrappers), len(reservation)))
    if not all(x.open for x in connectionwrappers.values()):
        printe('Not all provided connections are open.')
        return False


    retval = _execute_internal(connectionwrappers, reservation, key_path, paths, dest, silent, copy_multiplier, link_multiplier)
    if not use_local_connections:
        ssh_wrapper.close_wrappers(connectionwrappers)
    return retval