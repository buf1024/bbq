from os.path import dirname
from functools import partial
from bbq.common import init_facility, get_facility

__brokers = dict(builtin=dict(), external=dict())

init_broker = partial(init_facility, __brokers, (dirname(__file__), 'bbq.trade.broker', ('broker.py', )))
get_broker = partial(get_facility, __brokers)
