from .broker import Broker


class BrokerBacktest(Broker):
    def __init__(self, account=None):
        super().__init__(account)

    def name(self):
        return 'backtest'

    def on_strategy(self, payload):
        print('backtest on_strategy payload={}', payload)

    def on_quot(self, payload):
        super().on_quot(payload=payload)
