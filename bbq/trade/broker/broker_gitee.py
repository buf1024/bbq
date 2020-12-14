from .broker import Broker
from bbq.trade.account import Account
import copy


class BrokerGitee(Broker):
    def __init__(self, broker_id, account: Account):
        super().__init__(broker_id=broker_id, account=account)

    async def on_entrust(self, evt, payload):
        pass
