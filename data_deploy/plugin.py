from data_deploy.internal.platform.registrar import Registrar
from data_deploy.internal.platform.platform import register_plugins

def list():
    registrar = Registrar()
    register_plugins(registrar)
    print('Found {} plugins.'.format(len(registrar)))
    for plugin in registrar.plugins:
        print(
'''    Name: {}
    Path: {}
'''.format(plugin.name, plugin.path))
    return True