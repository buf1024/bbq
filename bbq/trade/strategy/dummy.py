from .strategy import Strategy
from ..account import Account
from ..trade_signal import TradeSignal
from datetime import datetime


class Dummy(Strategy):
    def __init__(self, strategy_id, account: Account):
        super().__init__(strategy_id=strategy_id, account=account)

        self.trade_date = {}

    async def on_quot(self, evt, payload):
        self.log.info('dummy strategy on_quot: evt={}, payload={}'.format(evt, payload))
        if evt == 'evt_quotation':
            for quot in payload['list'].values():
                day_time = quot['day_time']
                trade_date = datetime(year=day_time.year, month=day_time.month, day=day_time.day)
                code, price = quot['code'], quot['close']
                is_sig = False
                if trade_date not in self.trade_date:
                    self.trade_date[trade_date] = [code]
                    is_sig = True
                else:
                    codes = self.trade_date[trade_date]
                    if code not in codes:
                        codes.append(code)
                        is_sig = True

                if is_sig:
                    sig = TradeSignal(self.get_uuid(), self.account)
                    sig.source = 'strategy'
                    sig.signal = 'buy'
                    sig.code = code
                    sig.price = price
                    sig.volume = 100
                    sig.time = day_time
                    self.emit('signal', 'evt_signal', sig)
                    if sig.code.startswith('sz'):
                        self.emit('signal', 'evt_signal', sig)
