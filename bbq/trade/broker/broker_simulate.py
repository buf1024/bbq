from .broker import Broker
from bbq.trade.account import Account
import copy
import bbq.trade.consts as consts


class BrokerSimulate(Broker):
    def __init__(self, broker_id, account: Account):
        super().__init__(broker_id=broker_id, account=account)

    def name(self):
        return '神算子模拟券商'

    async def on_entrust(self, evt, payload):
        self.log.info('on entrust, evt={}, entrust={}'.format(evt, payload))
        entrust = copy.copy(payload)
        if evt == consts.evt_entrust_buy:
            entrust.broker_entrust_id = self.get_uuid()
            entrust.status = entrust.stat_deal
            entrust.volume_deal = entrust.volume
            await self.emit('broker_event', consts.evt_broker_deal, entrust)

        if evt == consts.evt_entrust_sell:
            entrust.broker_entrust_id = self.get_uuid()
            entrust.status = entrust.stat_deal
            entrust.volume_deal = entrust.volume
            await self.emit('broker_event', consts.evt_broker_deal, entrust)

        if evt == consts.evt_entrust_cancel:
            entrust.broker_entrust_id = self.get_uuid()
            entrust.status = entrust.stat_cancel
            entrust.volume_cancel = entrust.volume_cancel
            await self.emit('broker_event', consts.evt_broker_cancelled, entrust)
