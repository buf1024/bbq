from os.path import dirname
from functools import partial
from bbq.common import prepare_strategy, get_strategy

__strategies = dict(builtin=dict(), external=dict())

init_strategy = partial(prepare_strategy, __strategies, (dirname(__file__), 'bbq.monitor.strategy', ('strategy.py',)))


def get_strategies():
    strategies = []
    for v in __strategies.values():
        strategies = strategies + list(v.values())
    return strategies


get_strategy = partial(get_strategy, __strategies)
