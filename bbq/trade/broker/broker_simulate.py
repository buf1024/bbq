from .broker import Broker
from bbq.trade.account import Account
import copy
from bbq.trade import event


class BrokerSimulate(Broker):
    def __init__(self, broker_id, account: Account):
        super().__init__(broker_id=broker_id, account=account)

    def name(self):
        return '神算子模拟券商'

    async def on_entrust(self, evt, payload):
        entrust = copy.copy(payload)
        if evt == event.evt_entrust_buy:
            entrust.broker_entrust_id = self.get_uuid()
            entrust.status = 'deal'
            entrust.volume_deal = entrust.volume
            await self.emit('broker_event', event.evt_broker_deal, entrust)

        if evt == event.evt_entrust_sell:
            entrust.broker_entrust_id = self.get_uuid()
            entrust.status = 'deal'
            entrust.volume_deal = entrust.volume
            await self.emit('broker_event', event.evt_broker_deal, entrust)

        if evt == event.evt_entrust_cancel:
            entrust.broker_entrust_id = self.get_uuid()
            entrust.status = 'cancel'
            entrust.volume_cancel = entrust.volume_cancel
            await self.emit('broker_event', event.evt_broker_cancelled, entrust)
