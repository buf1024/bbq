from os.path import dirname
from functools import partial
from bbq.common import prepare_strategy, get_strategy

__brokers = dict(builtin=dict(), external=dict())

init_broker = partial(prepare_strategy, __brokers, (dirname(__file__), 'bbq.trade.broker', ('broker.py', )))
get_broker = partial(get_strategy, __brokers)
