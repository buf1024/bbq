from .broker import Broker
from bbq.trade.account import Account
import copy


class BrokerSimulate(Broker):
    def __init__(self, broker_id, account: Account):
        super().__init__(broker_id=broker_id, account=account)

        self.event = []

    async def on_entrust(self, evt, payload):
        entrust = copy.copy(payload)
        if evt == 'evt_broker_buy':
            entrust.broker_entrust_id = self.get_uuid()
            entrust.status = 'deal'
            entrust.volume_deal = entrust.volume
            self.emit('broker_event', 'evt_broker_deal', entrust)

        if evt == 'evt_broker_sell':
            entrust.broker_entrust_id = self.get_uuid()
            entrust.status = 'deal'
            entrust.volume_deal = entrust.volume
            self.emit('broker_event', 'evt_broker_deal', entrust)
