import data_deploy.internal.util.fs as fs
import data_deploy.internal.util.importer as importer


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
    

    def change_name(self, newname):
        self._name = newname


    def load(self):
        if self._loaded:
            raise RuntimeError('Cannot load plugin "{}": Already loaded.'.format(self._name))
        self._module = importer.import_full_path(abs_path)
        return self._module


    @staticmethod
    def basename(path):
        return fs.basename()[:-len(Plugin.plugin_extension)]


    def __str__(self):
        return self._name