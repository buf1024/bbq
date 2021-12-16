from os.path import dirname
from functools import partial
from bbq.common import init_facility, get_facility

__strategies = dict(builtin=dict(), external=dict())

init_strategy = partial(init_facility, __strategies, (dirname(__file__), 'bbq.trade.strategy', ('strategy.py',)))
get_strategy = partial(get_facility, __strategies)
