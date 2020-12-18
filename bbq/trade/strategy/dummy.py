from .strategy import Strategy
from ..account import Account
from ..trade_signal import TradeSignal
from datetime import datetime


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
        if evt == 'evt_quotation':
            for quot in payload['list'].values():
                day_time = quot['day_time']
                trade_date = datetime(year=day_time.year, month=day_time.month, day=day_time.day)
                code, name, price = quot['code'], quot['name'], quot['close']
                is_sig, signal = False, ''

                if code not in self.test_codes_buy:
                    is_sig = True
                    signal = 'buy'
                    self.test_codes_buy.append(code)
                    self.trade_date_buy[code] = trade_date
                if code not in self.test_codes_sell and code in self.test_codes_buy:
                    if self.trade_date_buy[code] != trade_date:
                        is_sig = True
                        signal = 'sell'
                        self.test_codes_sell.append(code)
                        self.trade_date_sell[code] = trade_date

                if is_sig:
                    sig = TradeSignal(self.get_uuid(), self.account)
                    sig.source = 'strategy:builtin:Dummy'
                    sig.source_desc = self.name()
                    sig.signal = signal
                    sig.code = code
                    sig.name = name
                    sig.price = price
                    sig.volume = 100
                    sig.time = day_time
                    await self.emit('signal', ('evt_sig_buy' if signal == 'buy' else 'evt_sig_sell'), sig)
