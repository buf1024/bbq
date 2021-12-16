from os.path import dirname
from functools import partial
from bbq.common import init_facility, get_facility

__risks = dict(builtin=dict(), external=dict())

init_risk = partial(init_facility, __risks, (dirname(__file__), 'bbq.trade.risk', ('risk.py', )))
get_risk = partial(get_facility, __risks)
