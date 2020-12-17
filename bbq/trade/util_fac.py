from bbq.common import load_strategy
import sys
import bbq.trade.broker as broker
import bbq.trade.risk as risk
import bbq.trade.robot as robot
import bbq.trade.strategy as strategy
from functools import partial

__all__ = ['init_broker', 'get_broker', 'init_risk', 'get_risk',
           'init_strategy', 'get_strategy', 'init_robot', 'get_robot']


def init_facility(data_dict, data_builtin, paths):
    builtin_dict = data_dict['builtin']
    if len(builtin_dict) == 0:
        strategy_data = load_strategy(*data_builtin)
        if len(strategy_data) > 0:
            builtin_dict.update(strategy_data)

    external_dict = data_dict['external']
    if paths is not None:
        for path in paths:
            if path not in sys.path:
                sys.path.append(path)
            strategy_data = load_strategy(path, '')
            if len(strategy_data) > 0:
                external_dict.update(strategy_data)


def get_facility(data_dict, name):
    name_pair = name.split(':')
    if len(name_pair) != 2:
        return None

    d = data_dict['builtin'] if name_pair[0] == 'builtin' else data_dict['external']

    if name_pair[1] not in d:
        return None

    return d[name_pair[1]]


init_broker = partial(init_facility, broker.brokers, broker.context)
init_risk = partial(init_facility, risk.risks, risk.context)
init_strategy = partial(init_facility, strategy.strategies, strategy.context)
init_robot = partial(init_facility, robot.robots, robot.context)

get_broker = partial(get_facility, broker.brokers)
get_risk = partial(get_facility, risk.risks)
get_strategy = partial(get_facility, strategy.strategies)
get_robot = partial(get_facility, robot.robots)
