from .risk import Risk
from ..account import Account


class Dummy(Risk):
    def __init__(self, risk_id, account: Account):
        super().__init__(risk_id=risk_id, account=account)

    def on_broker(self, payload):
        print('risk on_broker: {}'.format(payload))

    def on_quot(self, payload):
        print('risk on_quot: {}'.format(payload))
