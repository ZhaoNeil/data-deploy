import data_deploy.internal.util.fs as fs
import data_deploy.internal.util.importer as importer
from data_deploy.internal.util.printer import *

class Plugin(object):
    '''Container to hold a path reference to a plugin file. It can also load the plugin.'''


    # Extension to use for detecting plugin files.
    plugin_extension = '.deploy.plugin.py'

    
    def __init__(self, path, name=None):
        self._path = path
        self._name = name if name else Plugin.basename(path)
        self._loaded = False
        self._module = None


    @property
    def path(self):
        return self._path

    @property
    def name(self):
        return self._name

    @property
    def loaded(self):
        return self._loaded

    @property
    def module(self):
        if not self._loaded:
            self.load()
        return self._module

    @property
    def description(self):
        try:
            return self.module.description()
        except AttributeError as e:
            printw('Module "{}" had an error while obtaining description: {}'.format(self._name, e))
            return 'Module "{}" had an error while obtaining description: {}'.format(self._name, e)

    def parse(self, args):
        return self.module.parse(args)

    def execute(self, reservation, key_path, paths, dest, silent, *args, **kwargs):
        return self.module.execute(reservation, key_path, paths, dest, silent, *args, **kwargs)


    def change_name(self, newname):
        self._name = newname


    def load(self):
        if self._loaded:
            raise RuntimeError('Cannot load plugin "{}": Already loaded.'.format(self._name))
        self._module = importer.import_full_path(self._path)
        return self._module


    @staticmethod
    def basename(path):
        return fs.basename(path[:-len(Plugin.plugin_extension)])


    def __str__(self):
        return self._name