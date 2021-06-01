import concurrent.futures
import subprocess

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



def _deploy_internal(connectionwrapper, admin_node, reservation, paths, dest, strategy, silent, retries):
    if not silent:
        print('Transferring data...')

    with concurrent.futures.ThreadPoolExecutor(max_workers=len(wrappers)) as executor:
        if not remoto.process.check(connectionwrapper.connection, 'mkdir -p {}'.format(dest), shell=True)[2] == 0:
            printe('Could not create data destination directory on admin node.')
            return False

        fun = lambda path, node, wrapper: subprocess.call('rsync -e "ssh -F {}" -q -aHAX --inplace {} {}:{}'.format(wrapper.ssh_config_path, path, node.ip_public, fs.join(dest, fs.basename(path))), shell=True) == 0
        futures_rsync = [(executor.submit(fun, path, node, wrapper) for node, wrapper in wrappers.items()) for path in paths]
        if not all(x.result() for x in futures_rsync):
            printe('Could not transfer data to admin node.')
            return False
    star_nodes = [x for x in reservation.nodes if x != admin_node]
    star_cmd = '''python3 -c "
import concurrent.futures
import subprocess
hostnames = [{0}]
sourcedir = '{1}'
with concurrent.futures.ThreadPoolExecutor(max_workers=len(hostnames)) as executor:
    futures_mkdir = [executor.submit(subprocess.call, 'ssh {{}} "mkdir -p {{}}"'.format(hostname, sourcedir), shell=True) for x in hostnames]
    if not all(x.result() == 0 for x in futures_mkdir):
        print('Could not create data destination directory for all nodes.')
        exit(1)
    futures_rsync = [executor.submit(subprocess.call, 'rsync -q -aHAX --inplace {{}} {{}}:{{}}'.format(sourcedir, hostname, sourcedir), shell=True) for hostname in hostnames]
    if not all(x.result() for x in futures_rsync):
        print('Could not transfer data to some nodes.')
        exit(1)
exit(0)
"
'''.format(','.join("'{}'".format(x.hostname) for x in star_nodes), dest)
    out, error, exitcode = remoto.process.check(connectionwrapper.connection, star_cmd, shell=True)
    if exitcode == 0:
        return True
    printe('Could not transfer data from admin to all other nodes. Exitcode={}.\nOut={}\nError={}'.format(exitcode, out, error))
    return False



def deploy(reservation, *args, connectionwrapper=None, key_path=None, admin_id=None, paths=[], dest=defaults.remote_dir(), strategy=defaults.strategy(), silent=False, retries=defaults.retries(), **kwargs):
    admin_node, _ = _pick_admin(reservation, admin=admin_id)

    use_local_connections = connectionwrapper == None
    if use_local_connections: # We did not get any connections, so we must make them
        ssh_kwargs = {'IdentitiesOnly': 'yes', 'StrictHostKeyChecking': 'no'}
        if key_path:
            ssh_kwargs['IdentityFile'] = key_path

        connectionwrapper = ssh_wrapper.get_wrapper(admin_node, admin_node.ip_public, ssh_params=_merge_kwargs(ssh_kwargs, {'User': admin_node.extra_info['user']}), silent=silent)
    else: # We received connections, need to check if they are valid.
        if not connectionwrapper.open:
            raise ValueError('Provided connection is not open.')

    retval = _deploy_internal(connectionwrapper, admin_node, reservation, paths, dest, strategy, silent, retries)
    if not use_local_connections:
        _close_connections(connectionwrappers)
    return retval