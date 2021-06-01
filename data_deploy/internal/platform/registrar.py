from data_deploy.internal.platform.plugin import Plugin

class Registrar(object):
    '''Registrar for plugins.'''

    def __init__(self):
        self._register = dict()


    @property
    def plugin_names(self):
        return [x.name for x in self._register]


    def get(self, name):
        return self._register[name]


    def register(path):
        '''Registers given path as a module.'''
        plugin = Plugin(path)
        if plugin.name in self._register:
            print('Plugin "{}" already exists in registry. Registered as "{}"'.format(plugin.name, plugin.name+'0'))
            plugin.change_name(plugin.name+'0')
        self._register[plugin.name] = plugin


    def __len__(self):
        return len(self._register)