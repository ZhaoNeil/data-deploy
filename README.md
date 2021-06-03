# data-deploy
Framework and CLI-tool to deploy data on remote clusters. 

This tool is plugin-based.
Every plugin receives a set of standard arguments, and they can define and accept custom arguments, even when using the CLI.

A few basic plugins have been implemented already, in [implementations](/implementations/).



## Requirements
 - Python>=3.2
 - remoto>=1.2.0
 - metareserve>=0.1.0


## Installing
Simply execute `pip3 install . --user` in the root directory.


## Usage
After successful installation, a CLI program `data-deploy` is available. 
It has 2 primary options:
 1. `data-deploy plugin`: This command displays all found plugins, together with origin, a description and a path to the plugin location.
 2. `data-deploy deploy <standard_args> <plugin_name> -- <args>`: This command executes `<plugin_name>`, using standard arguments `<standard_args>` and plugin-specific arguments `<args>`.

For more information, see:
```bash
data-deploy -h
```


## Plugins
A plugin is a Python file (or multiple files) with a name ending on `.deploy.plugin.py`.
Inside such a file, 4 functions must be implemented:
```python
def description():
    return 'A super-handy plugin. Best used when local->remote connections are a bottleneck.'


def origin():
    return 'A simple plugin, coming from the example application.'


def parse(args):
    import argparse
    parser = argparse.ArgumentParser(prog='...')
    parser.add_argument('--foo', help='If set, will do a foo.')
    parser.add_argument('--bar', help='If set,  will bar.')
    args = parser.parse_args(args)
    return True, [args.foo], {'bar': args.bar}


def execute(reservation, key_path, paths, dest, silent, *args, **kwargs):
    foo = args[0]
    bar = kwargs['bar']
    ...
    return True # Execution was a success.
```
 - The `description()` and `origin()` functions should provide a description and origin message, respectively.
 - The `parse(args)` function receives a list of arguments to parse. We highly recommend using `argparse`, because the provided `args` are formatted such that `argparse` can do its magic.
 - The `execute(reservation, key_path, paths, dest, silent, *args, **kwargs)` function should perform the data deployment. The provided parameters are:
    - `reservation`: a `metareserve.Reservation` object, containing all nodes the data must be sent to.
    - `key_path`: A `str` path pointing to a SSH keyfile to use for connections, or `None`.
    - `paths`: An iterable of `str` paths to files or directories to transfer. When pointing to a directory, the directory and all its contents should be transferred.
    - `dest`: The destination path on the remote. If the path does not exist on the remote, it should be created.


### Plugin Locations
Plugins are only searched for in 2 locations:
 1. The [implementations](/implementations/) directory.
 2. The `~/.data_deploy/` directory.

Adding a plugin is handled most easily and gracefully by adding a symlink in `~/.data_deploy/`, pointing to a valid plugin file. 