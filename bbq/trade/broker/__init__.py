from os.path import dirname

brokers = dict(builtin=dict(), external=dict())
context = dirname(__file__), 'bbq.trade.broker', ('broker.py', )
