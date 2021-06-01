import data_deploy.plugin as plugin

from data_deploy.internal.util.printer import *


'''CLI module to list plugins for this tool.'''

def subparser(subparsers):
    '''Register subparser modules'''
    pluginparser = subparsers.add_parser('plugin', help='plugin data on a cluster.')
    return [pluginparser]


def deploy_args_set(args):
    '''Indicates whether we will handle command parse output in this module.
    `plugin()` function will be called if set.

    Returns:
        `True` if we found arguments used by this subsubparser, `False` otherwise.'''
    return args.command == 'plugin'

def deploy(parsers, args):
    return plugin.list()