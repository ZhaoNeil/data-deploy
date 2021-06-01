from data_deploy.internal.platform.plugin import Plugin

class Registrar(object):
    '''Registrar for plugins.'''

    def __init__(self):
        self._register = dict()


    @property
    def names(self):
        return [x for x in self._register.keys()]

    @property
    def plugins(self):
        return [x for x in self._register.values()]


    def get(self, name):
        return self._register[name]


    def register(self, path):
        '''Registers given path as a module.'''
        plugin = Plugin(path)
        if plugin.name in self._register:
            print('Plugin "{}" already exists in registry. Registered as "{}"'.format(plugin.name, plugin.name+'0'))
            plugin.change_name(plugin.name+'0')
        self._register[plugin.name] = plugin


    def __len__(self):
        return len(self._register)