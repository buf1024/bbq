from .strategy import Strategy
from ..account import Account
from ..trade_signal import TradeSignal
from copy import copy
import bbq.trade.consts as consts


class Dummy(Strategy):
    def __init__(self, strategy_id, account: Account):
        super().__init__(strategy_id=strategy_id, account=account)

        self.test_codes_buy = []
        self.test_codes_sell = []

        self.trade_date_buy = {}
        self.trade_date_sell = {}

    def name(self):
        return '神算子Dummy策略'

    async def on_quot(self, evt, payload):
        self.log.info('dummy strategy on_quot: evt={}, payload={}'.format(evt, payload))
        if evt == consts.evt_quotation:
            for quot in payload['list'].values():
                code, name, price = quot['code'], quot['name'], quot['close']
                if self.is_index(code):
                    continue

                # day_time = quot['day_time']
                # sig = TradeSignal(self.get_uuid(), self.account,
                #                   source=self.get_obj_id(typ=TradeSignal.strategy), source_name=self.name(),
                #                   code=code, name=name, price=price, time=day_time)
                #
                # if self.can_buy(code=code, price=price, volume=200):
                #     sig.signal = sig.sig_buy
                #     sig.volume = 200
                #     await self.buy(sig=sig)
                #
                # vol = self.can_sell_volume(code=code)
                # if vol > 0:
                #     sig = copy(sig)
                #     sig.signal = sig.sig_sell
                #     sig.volume = 100 if vol <= 100 else int(vol / 2)
                #     await self.sell(sig=sig)
