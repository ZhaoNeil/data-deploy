import data_deploy.internal.defaults.deploy as defaults
import data_deploy.cli.util as _cli_util


'''CLI module to deploy data on a cluster.'''

def subparser(subparsers):
    '''Register subparser modules'''
    deployparser = subparsers.add_parser('deploy', help='deploy data on a cluster.')
    deployparser.add_argument('plugin', metavar='name', type=str, help='Plugin to use for deployment. Use "data-deploy plugin" to list available plugins.')
    # deployparser.add_argument('paths', metavar='paths', nargs='+', help='Data to transmit to the remote. Pointed locations can be files or directories. Separate locations using spaces.')
    # deployparser.add_argument('--dest', metavar='path', type=str, default=defaults.remote_dir(), required=True, help='Destination path on host (default={}). Any non-existing directories on the remote will be created.'.format(defaults.remote_dir()))
    
    # deployparser.add_argument('--admin', metavar='id', dest='admin_id', type=int, default=None, help='ID of the node that will be the primary or admin node, for strategies using those.')
    # deployparser.add_argument('--strategy', metavar='strategy', type=str, default=defaults.strategy().name.lower(), help='Strategy to deploy data (default={}).'.format(defaults.strategy().name.lower()))
    # deployparser.add_argument('--silent', help='If set, less boot output is shown.', action='store_true')
    # deployparser.add_argument('--retries', metavar='amount', type=int, default=defaults.retries(), help='Amount of retries to use for risky operations (default={}).'.format(defaults.retries()))
    # return [deployparser]


def deploy_args_set(args):
    '''Indicates whether we will handle command parse output in this module.
    `deploy()` function will be called if set.

    Returns:
        `True` if we found arguments used by this subsubparser, `False` otherwise.'''
    return args.command == 'deploy'

def deploy(parsers, args):
    reservation = _cli_util.read_reservation_cli()
    return deploy(reservation, key_path=args.key_path, admin_id=args.admin_id, paths=args.paths, dest=args.dest, strategy=args.strategy, silent=args.silent, retries=args.retries) if reservation else False