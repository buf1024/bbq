from bbq.common import load_strategy
from os.path import dirname
import sys

brokers = dict(builtin=dict(), external=dict())


def init_broker(paths):
    builtin_dict = brokers['builtin']
    if len(builtin_dict) == 0:
        file_path = dirname(__file__)
        strategy = load_strategy(file_path, 'bbq.trade.broker', ('broker.py', ))
        if len(strategy) > 0:
            builtin_dict.update(strategy)

    external_dict = brokers['external']
    if paths is not None:
        for path in paths:
            if path not in sys.path:
                sys.path.append(path)
            strategy = load_strategy(path, '')
            if len(strategy) > 0:
                external_dict.update(strategy)


def get_broker(name):
    name_pair = name.split(':')
    if len(name_pair) != 2:
        return None

    d = brokers['builtin'] if name_pair[0] == 'builtin' else brokers['external']

    if name_pair[1] not in d:
        return None

    return d[name_pair[1]]

