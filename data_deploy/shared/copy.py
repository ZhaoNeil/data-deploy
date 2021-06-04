import remoto


def copy_expression(sourcefile, num_copies):
    '''Returns a generator expression for the sourcefile and all copies.'''
    return '''import itertools
files = itertools.chain(['{0}'], ('{0}.copy.{{}}'.format(x) for x in range({1})))'''.format(sourcefile, num_copies)
        


def copy_single(connection, sourcefile, num_copies, silent=False):
    '''Copies a file `num_copies` times. For a file named "X", we generate copies named "X.copy.0", "X.copy.1", ....
    Args:
        connection (remoto.Connection): Connection to remote to execute on.
        sourcefile (str): Path to file to copy.
        num_copies (int): Amount of copies to generate.
        silent (optional bool): If set, never prints. Otherwise, prints on error.'''
    if num_copies > 0:
        cmd = '''python3 -c "
import shutil
srcloc = '{0}'
for x in range({1}):
    dstloc = '{0}.copy.{{}}'.format(x)
    shutil.copyfile(srcloc, dstloc)
exit(0)
"
'''.format(sourcefile, num_copies)
        out, error, exitcode = remoto.process.check(connection, cmd, shell=True)
        if exitcode != 0:
            if not silent:
                printe('Could not add copies for file: {}.\nReason: Out: {}\n\nError: {}'.format(sourcefile, '\n'.join(out), '\n'.join(error)))
            return False
    return True