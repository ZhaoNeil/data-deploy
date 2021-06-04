import remoto


def link_single(connection, sourcefile, num_links=1, silent=False):
    '''Makes a hardlink to a file `num_links` times. For a file named "X", we generate hardlinks named "X.link.0", "X.link.1", ....
    Args:
        connection (remoto.Connection): Connection to remote to execute on.
        sourcefile (str): Path to file to make hardlinks for.
        num_links (int): Amount of hardlinks to generate.
        silent (optional bool): If set, never prints. Otherwise, prints on error.'''
    if num_links > 0:
        cmd = '''python3 -c "
import itertools
import os
pointedloc = '{0}'
for x in range({1}):
    pointerloc = '{{}}.link.{{}}'.format(pointedloc, x)
    if os.path.exists(pointerloc):
        os.remove(pointerloc)
    os.link(pointedloc, pointerloc)
exit(0)
"
'''.format(sourcefile, num_links)
        out, error, exitcode = remoto.process.check(connection, cmd, shell=True)
        if exitcode != 0:
            if not silent:
                printe('Could not add hardlinks for file: {}.\nReason: Out: {}\n\nError: {}'.format(dest_file, '\n'.join(out), '\n'.join(error)))
            return False
    return True


def link(connection, files=None, expression=None, num_links=1, silent=True):
    '''Provide hardlinks for multiple files. There are 2 ways to do this:
     1. Specify the files to link for. If using hundredthousands of files, the command here will become quite inflated in size.
     2. Use an expression. The filenames are then generated at the side of the destination, which means no inflation here.
     Args:
        connection (remoto.Connection): Connection to remote to execute on.
        files (optional iterable(str)): If set, uses provided set of files as file sources.
        expression (optional str): If set, uses provided expression as file sources. Expression must set a 'files' variable to an iterable.
        num_links (optional int): Amount of links to generate.
        silent (optional bool): If set, never prints. Otherwise, prints on error.

    Returns:
        `True` on success, `False` on failure.'''
    if ((not files) and (not expression)) or (files and expression):
        raise ValueError('Caller must specify either files or expression. Currently specifies {}.'.format('both' if files else 'none'))

    if num_links > 0:
        if files:
            expression = 'files = ['+ ','.join("'{}'".format(file) for file in files) + ']'
        cmd = '''python3 -c "
import itertools
import os
num_links = {1}
{0}
for pointedloc in files:
    for x in range(num_links):
        pointerloc = '{{}}.link.{{}}'.format(pointedloc, x)
        if os.path.exists(pointerloc):
            os.remove(pointerloc)
        os.link(pointedloc, pointerloc)
exit(0)
"
'''.format(expression, num_links)
        out, error, exitcode = remoto.process.check(connection, cmd, shell=True)
        if exitcode != 0:
            if not silent:
                printe('Could not add hardlinks for file: {}.\nReason: Out: {}\n\nError: {}'.format(dest_file, '\n'.join(out), '\n'.join(error)))
            return False
    return True