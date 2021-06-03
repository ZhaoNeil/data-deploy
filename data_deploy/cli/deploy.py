from data_deploy.deploy import deploy_cli
import data_deploy.internal.defaults.deploy as defaults


'''CLI module to deploy data on a cluster.'''

def subparser(subparsers):
    '''Register subparser modules'''
    deployparser = subparsers.add_parser('deploy', help='deploy data on a cluster.')
    deployparser.add_argument('--paths', metavar='paths', nargs='+', help='Data to transmit to the remote. Pointed locations can be files or directories. Separate locations using spaces.')
    deployparser.add_argument('--dest', metavar='path', type=str, default=defaults.remote_dir(), help='Destination path on host (default={}). Any non-existing directories on the remote will be created.'.format(defaults.remote_dir()))
    deployparser.add_argument('--copy-multiplier', metavar='amount', dest='copy_multiplier', type=int, default=1, help='Copy multiplier (default=1). Every file will be copied "amount"-1 times on the remote, to make the data look "amount" times larger. This multiplier is applied first.')
    deployparser.add_argument('--link-multiplier', metavar='amount', dest='link_multiplier', type=int, default=1, help='Link multiplier (default=1). Every file will receive "amount"-1 hardlinks on the remote, to make the data look "amount" times larger. This multiplier is applied second. Note that we first apply the copy multiplier, meaning: the link multiplier is applied on copies of files, i.e. the dataset inflation stacks.')

    deployparser.add_argument('--silent', help='If set, less boot output is shown.', action='store_true')
    deployparser.add_argument('plugin', metavar='name', type=str, help='Plugin to use for deployment. Use "data-deploy plugin" to list available plugins.')
    deployparser.add_argument('args', metavar='args', nargs='*', help='Arguments for the plugin. Use "data-deploy <plugin_name> -- -h" to see possible arguments.')
    return [deployparser]


def deploy_args_set(args):
    '''Indicates whether we will handle command parse output in this module.
    `deploy()` function will be called if set.

    Returns:
        `True` if we found arguments used by this subsubparser, `False` otherwise.'''
    return args.command == 'deploy'


def deploy(parsers, args):
    return deploy_cli(key_path=args.key_path, paths=args.paths, dest=args.dest, silent=args.silent, copy_multiplier=args.copy_multiplier, link_multiplier=args.link_multiplier, plugin=args.plugin, args=args.args)