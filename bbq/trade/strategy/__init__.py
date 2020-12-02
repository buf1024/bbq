from bbq.common import load_strategy
from os.path import dirname
import sys

strategies = dict(builtin=dict(), external=dict())


def init_strategy(paths):
    builtin_dict = strategies['builtin']
    if len(builtin_dict) == 0:
        file_path = dirname(__file__)
        strategy = load_strategy(file_path, 'trader.strategy', ('strategy.py', ))
        if len(strategy) > 0:
            builtin_dict.update(strategy)

    external_dict = strategies['external']
    for path in paths:
        if path not in sys.path:
            sys.path.append(path)
        strategy = load_strategy(path, '')
        if len(strategy) > 0:
            external_dict.update(strategy)


def get_strategy(name):
    name_pair = name.split(':')
    if len(name_pair) != 2:
        return None

    d = strategies['builtin'] if name_pair[0] == 'builtin' else strategies['external']

    if name_pair[1] not in d:
        return None

    return d[name_pair[1]]

