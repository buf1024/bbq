from .risk import Risk
from ..account import Account


class Dummy(Risk):
    def __init__(self, risk_id, account: Account):
        super().__init__(risk_id=risk_id, account=account)

    async def on_quot(self, evt, payload):
        self.log.info('dummy risk on_quot: evt={}, payload={}'.format(evt, payload))
