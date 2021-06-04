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


'''Deploys data by sending all data from the local machine to one remote (the 'admin') in parallel. The admin then sends all data in parallel to all other nodes.
Works well if local->local connections don't bottleneck.'''

def _merge_kwargs(x, y):
    z = x.copy()
    z.update(y)
    return z


def _pick_admin(reservation, admin=None):
    '''Picks an admin node.
    Args:
        reservation (`metareserve.Reservation`): Reservation object to pick admin from.
        admin (optional int): If set, picks node with given `node_id`. Picks node with lowest public ip value, otherwise.

    Returns:
        admin, list of non-admins.'''
    if len(reservation) == 1:
        return next(reservation.nodes), []

    if admin:
        return reservation.get_node(node_id=admin), [x for x in reservation.nodes if x.node_id != admin]
    else:
        tmp = sorted(reservation.nodes, key=lambda x: x.ip_public)
        return tmp[0], tmp[1:]


def _execute_internal(wrappers, admin_node, reservation, paths, dest, silent, copy_multiplier, link_multiplier):
    if not silent:
        print('Transferring data...'.format(copy_multiplier, link_multiplier))

    with concurrent.futures.ThreadPoolExecutor(max_workers=len(wrappers)) as executor:
        if not remoto.process.check(wrappers[admin_node].connection, 'mkdir -p {}'.format(dest), shell=True)[2] == 0:
            printe('Could not create data destination directory on admin node.')
            return False

        fun = lambda path, node, wrapper: subprocess.call('rsync -e "ssh -F {}" -q -aHAX --inplace {} {}:{}'.format(wrapper.ssh_config_path, path, node.ip_public, fs.join(dest, fs.basename(path))), shell=True) == 0
        futures_rsync = [executor.submit(fun, path, admin_node, wrappers[admin_node]) for path in paths]
        if not all(x.result() for x in futures_rsync):
            if not silent:
                printe('Could not transfer data to admin node.')
            return False
        star_nodes = [x for x in reservation.nodes if x != admin_node]
        paths_remote = [fs.join(dest, fs.basename(path)) for path in paths]
        star_cmd = '''python3 -c "
import concurrent.futures
import subprocess
hostnames = [{0}]
sourcedir = '{1}'
paths_remote = [{2}]
with concurrent.futures.ThreadPoolExecutor(max_workers=len(hostnames)) as executor:
    futures_mkdir = [executor.submit(subprocess.call, 'ssh {{}} \\'mkdir -p {{}}\\''.format(x, sourcedir), shell=True) for x in hostnames]
    if not all(x.result() == 0 for x in futures_mkdir):
        print('Could not create data destination directory for all nodes.')
        exit(1)
    futures_rsync = [executor.submit(subprocess.call, 'rsync -q -aHAX --inplace {{}} {{}}:{{}}'.format(path, hostname, path), shell=True) for hostname in hostnames for path in paths_remote]
    if not all(x.result() == 0 for x in futures_rsync):
        print('Could not transfer data to some nodes.')
        exit(1)
exit(0)
    "
    '''.format(
        ','.join("'{}'".format(x.hostname) for x in star_nodes),
        dest,
        ','.join("'{}'".format(x) for x in paths_remote))

        out, error, exitcode = remoto.process.check(wrappers[admin_node].connection, star_cmd, shell=True)
        if exitcode != 0:
            if not silent:
                printe('Could not transfer data from admin to all other nodes. Exitcode={}.\nOut={}\nError={}'.format(exitcode, out, error))
            return False

        copies_amount = max(1, copy_multiplier) - 1
        links_amount = max(1, link_multiplier) - 1
        if copies_amount > 0:
            futures_copy = [executor.submit(data_deploy.shared.copy.copy_single, wrapper.connection, path, copies_amount, silent=False) for wrapper in wrappers.values() for path in paths_remote]
            if not all(x.result() for x in futures_copy):
                if not silent:
                    printe('Could not create copies on all nodes.')
                return False
        if links_amount > 0:
            futures_link = [executor.submit(data_deploy.shared.link.link, wrapper.connection, expression=data_deploy.shared.copy.copy_expression(path, copies_amount), num_links=links_amount, silent=False) for wrapper in wrappers.values() for path in paths_remote]
            if not all(x.result() for x in futures_link):
                if not silent:
                    printe('Could not create links on all nodes.')
                return False
    return True


def description():
    return "Deploys data by sending all data from the local machine to one remote (the 'admin') in parallel. The admin then sends all data in parallel to all other nodes. Works well if local->local connections don't bottleneck."


def origin():
    return "Default implementation."


def parse(args):
    parser = argparse.ArgumentParser(prog='...')
    parser.add_argument('--admin', metavar='id', dest='admin_id', type=int, default=None, help='ID of the node that will be the primary or admin node.')
    args = parser.parse_args(args)
    return True, [], {'admin_id': args.admin_id}


def execute(reservation, key_path, paths, dest, silent, copy_multiplier, link_multiplier, *args, **kwargs):
    connectionwrappers = kwargs.get('connectionwrappers')
    admin_id = kwargs.get('admin_id')

    admin_node, _ = _pick_admin(reservation, admin=admin_id)
    use_local_connections = connectionwrappers == None
    if use_local_connections: # We did not get any connections, so we must make them
        ssh_kwargs = {'IdentitiesOnly': 'yes', 'StrictHostKeyChecking': 'no'}
        if key_path:
            ssh_kwargs['IdentityFile'] = key_path
        connectionwrappers = ssh_wrapper.get_wrappers(reservation.nodes, lambda node: node.ip_public, ssh_params=lambda node: _merge_kwargs(ssh_kwargs, {'User': node.extra_info['user']}), silent=silent)
    else: # We received connections, need to check if they are valid.
        if not all(x.open for x in connectionwrappers):
            raise ValueError('Not all provided connections are open.')

    retval = _execute_internal(connectionwrappers, admin_node, reservation, paths, dest, silent, copy_multiplier, link_multiplier)
    if not use_local_connections:
        ssh_wrapper.close_wrappers(connectionwrappers)
    return retval