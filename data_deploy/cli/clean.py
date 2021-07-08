from data_deploy.clean import clean
import data_deploy.cli.util as _cli_util
import data_deploy.internal.defaults.deploy as defaults


'''CLI module to clean data on a cluster.'''

def subparser(subparsers):
    '''Register subparser modules'''
    cleanparser = subparsers.add_parser('clean', help='Clean data on a cluster.')
    cleanparser.add_argument('--paths', metavar='paths', nargs='+', default=[defaults.remote_dir()], help='Remote paths to remove  (default={}). Wildcards are forwarded. Pointed locations can be files or directories. Separate locations using spaces.'.format(defaults.remote_dir()))
    cleanparser.add_argument('--sudo', help='If set, uses sudo to clean.', action='store_true')
    cleanparser.add_argument('--silent', help='If set, less boot output is shown.', action='store_true')
    return [cleanparser]


def deploy_args_set(args):
    '''Indicates whether we will handle command parse output in this module.
    `deploy()` function will be called if set.

    Returns:
        `True` if we found arguments used by this subsubparser, `False` otherwise.'''
    return args.command == 'clean'


def deploy(parsers, args):
    reservation = _cli_util.read_reservation_cli()
    return clean(reservation, key_path=args.key_path, paths=args.paths, sudo=args.sudo, silent=args.silent) if reservation else False