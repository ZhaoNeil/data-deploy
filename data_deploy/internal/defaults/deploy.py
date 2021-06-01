from rados_deploy import Strategy

def remote_dir():
    '''Default directory to export data to.'''
    return '~/data'


def strategy():
    return Strategy.REMOTE_STAR


def retries():
    5