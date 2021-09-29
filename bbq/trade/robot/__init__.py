from os.path import dirname
from functools import partial
from bbq.common import init_facility, get_facility

__all__ = ['init_robot', 'get_robot']

__robots = dict(builtin=dict(), external=dict())

init_robot = partial(init_facility, __robots, (dirname(__file__), 'bbq.trade.robot', ('robot.py', )))
get_robot = partial(get_facility, __robots)

