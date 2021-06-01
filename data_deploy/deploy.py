


def deploy(reservation, key_path=None, admin_id=None, paths=[], dest=defaults.remote_dir(), strategy=defaults.strategy(), silent=False, retries=defaults.retries()):
    strategy = strategy.upper()
    if not strategy in Strategy:
        raise ValueError('Given strategy "{}" is not a valid strategy. Options: {}'.format(strategy.lower(), ', '.join(x.name.lower() for x in strategy))

