import data_deploy.cli.util as _cli_util
import data_deploy.internal.defaults.deploy as defaults
from data_deploy.internal.platform.registrar import Registrar
from data_deploy.internal.platform.platform import register_plugins


def deploy_cli(key_path=None, paths=[], dest=defaults.remote_dir(), silent=False, plugin=None, args=None):
    '''Deploy data using the CLI. Plugins can take more arguments than the standard set we predefined.'''
    registrar = Registrar()
    register_plugins(registrar)
    print('Found {} plugins.'.format(len(registrar)))
    
    if not plugin in registrar.names:
        printe('Could not find a plugin named "{}"'.format(plugin))
        return False
    plugin = registrar.get(plugin)
    state, args, kwargs = plugin.parse(args)
    if not state:
        printe('Could not parse provided arguments: {}'.format(args))
        return False
    reservation = _cli_util.read_reservation_cli()
    if not reservation:
        return False
    return plugin.execute(reservation, key_path, paths, dest, silent, *args, **kwargs)


def deploy(reservation, key_path=None, paths=[], dest=defaults.remote_dir(), silent=False, plugin=None, *args, **kwargs):
    registrar = Registrar()
    register_plugins(registrar)
    print('Found {} plugins.'.format(len(registrar)))
    
    if not plugin in registrar.names:
        printe('Could not find a plugin named "{}"'.format(plugin))
        return False
    plugin = registrar.get(plugin)
    return True