from data_deploy.deploy import deploy_cli
import data_deploy.internal.defaults.deploy as defaults


'''CLI module to deploy data on a cluster.'''

def subparser(subparsers):
    '''Register subparser modules'''
    deployparser = subparsers.add_parser('deploy', help='deploy data on a cluster.')
    deployparser.add_argument('--paths', metavar='paths', nargs='+', help='Data to transmit to the remote. Pointed locations can be files or directories. Separate locations using spaces.')
    deployparser.add_argument('--dest', metavar='path', type=str, default=defaults.remote_dir(), help='Destination path on host (default={}). Any non-existing directories on the remote will be created.'.format(defaults.remote_dir()))

    deployparser.add_argument('--silent', help='If set, less boot output is shown.', action='store_true')
    deployparser.add_argument('plugin', metavar='name', type=str, help='Plugin to use for deployment. Use "data-deploy plugin" to list available plugins.')
    deployparser.add_argument('args', metavar='args', nargs='+', help='Arguments for the plugin. Use "data-deploy <plugin_name> -- -h" to see possible arguments.')
    return [deployparser]


def deploy_args_set(args):
    '''Indicates whether we will handle command parse output in this module.
    `deploy()` function will be called if set.

    Returns:
        `True` if we found arguments used by this subsubparser, `False` otherwise.'''
    return args.command == 'deploy'


def deploy(parsers, args):
    return deploy_cli(key_path=args.key_path, paths=args.paths, dest=args.dest, silent=args.silent, plugin=args.plugin, args=args.args)