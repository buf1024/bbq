from os.path import dirname

strategies = dict(builtin=dict(), external=dict())
context = dirname(__file__), 'bbq.trade.strategy', ('strategy.py', )
