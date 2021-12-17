from .risk import Risk
from ..account import Account
from ..trade_signal import TradeSignal
import bbq.trade.consts as consts


class SimpleStop(Risk):
    def __init__(self, risk_id, account: Account):
        super().__init__(risk_id=risk_id, account=account)

        self.stop_profit = 0
        self.stop_profit_rate = 0

        self.stop_lost = 0
        self.stop_lost_rate = 0.15

    def name(self):
        return '神算子止损止盈风控'

    async def init(self, opt):
        if opt is not None:
            self.stop_profit = 0 if 'stop_profit' not in opt else float(opt['stop_profit'])
            self.stop_lost_rate = 0 if 'stop_lost_rate' not in opt else float(opt['stop_lost_rate'])

            self.stop_lost = 0 if 'stop_lost' not in opt else float(opt['stop_lost'])
            self.stop_lost_rate = 0 if 'stop_lost_rate' not in opt else float(opt['stop_lost_rate'])

        return True

    async def on_quot(self, evt, payload):
        # self.log.info('simple stop risk on_quot: evt={}, payload={}'.format(evt, payload))
        for position in self.account.position.values():
            if position.volume_available <= 0:
                continue

            is_at_risk, is_at_profit = False, False
            sell_price = position.now_price

            is_lost = False if position.profit > 0 else True
            profit_rate = abs(position.profit_rate)
            profit = abs(position.profit)

            # 止损
            if is_lost and self.stop_lost_rate > 0 and self.stop_lost_rate > profit_rate:
                self.log.info(
                    'position({}) is at risk(stop_lost_rate=-{}, profit_rate=-{}), trigger sell signal'.format(
                        position.code, self.stop_lost_rate, profit_rate))
                # if profit_rate > (self.stop_lost_rate * 1.1):
                #     sell_price = position.now_price + 0.01
                is_at_risk = True

            if not is_at_risk:
                if is_lost and self.stop_lost > 0 and self.stop_lost > profit:
                    self.log.info(
                        'position({}) is at risk(stop_lost=-{}, profit=-{}), trigger sell signal'.format(
                            position.code, self.stop_lost, profit))
                    # if profit > (self.stop_lost * 1.1):
                    #     sell_price = position.now_price + 0.01
                    is_at_risk = True

            # 止盈
            if not is_lost and self.stop_profit_rate > 0 and self.stop_profit_rate > profit_rate:
                self.log.info(
                    'position({}) is at profit(stop_profit_rate={}, profit_rate={}), trigger sell signal'.format(
                        position.code, self.stop_profit_rate, profit_rate))
                # if profit_rate > (self.stop_lost_rate * 1.1):
                #     sell_price = position.now_price + 0.01
                is_at_profit = True

            if not is_at_profit:
                if not is_lost and self.stop_profit > 0 and self.stop_profit > profit:
                    self.log.info(
                        'position({}) is at profit(stop_profit_rate={}, profit_rate={}), trigger sell signal'.format(
                            position.code, self.stop_profit_rate, profit_rate))
                    # if profit > (self.stop_lost * 1.1):
                    #     sell_price = position.now_price + 0.01
                    is_at_profit = True

            # 触发信号
            if is_at_risk or is_at_profit:
                sig = TradeSignal(self.get_uuid(), self.account)
                sig.source = self.get_obj_id(typ=sig.risk)
                sig.source_name = self.name()
                sig.signal = sig.sig_sell
                sig.code = position.code
                sig.price = sell_price
                sig.volume = position.volume
                sig.time = position.time
                sig.desc = '止盈' if is_at_profit else '止损'
                await self.emit('signal', consts.evt_sig_sell, sig)
