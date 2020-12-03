from .broker import Broker
from bbq.trade.account import Account


class BrokerSimulate(Broker):

    def __init__(self, broker_id, account: Account):
        super().__init__(broker_id=broker_id, account=account)

