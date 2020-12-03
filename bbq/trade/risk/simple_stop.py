from .risk import Risk
from ..account import Account


class SimpleStop(Risk):
    def __init__(self, risk_id, account: Account):
        super().__init__(risk_id=risk_id, account=account)

        self.stop_profit = 0
        self.stop_profit_rate = 0

        self.stop_lost = 0
        self.stop_lost_rate = 0

        self.stop_time = 0

    async def init(self, opt):
        if opt is not None:
            self.stop_profit = 0 if 'stop_profit' not in opt else opt['stop_profit']
            self.stop_lost_rate = 0 if 'stop_lost_rate' not in opt else opt['stop_lost_rate']

            self.stop_lost = 0 if 'stop_lost' not in opt else opt['stop_lost']
            self.stop_lost_rate = 0 if 'stop_lost_rate' not in opt else opt['stop_lost_rate']

            self.stop_time = 0 if 'stop_time' not in opt else opt['stop_time']

        return True
        
    async def destroy(self):
        pass

    def on_broker(self, payload):
        print('risk on_broker: {}'.format(payload))

    def on_quot(self, payload):
        for position in self.account.position.values():
            if position.volume_available <= 0:
                continue

            is_lost = False if position.profit > 0 else True

            profit_rate = abs(position.profit_rate)
            profit = abs(position.profit)
            if self.stop_lost_rate > 0 and is_lost and self.stop_lost_rate > profit_rate:
                continue

            if self.stop_lost > 0 and is_lost and self.stop_lost > profit:
                continue

            if self.stop_profit_rate > 0 and not is_lost and self.stop_profit_rate > profit_rate:
                continue

            if self.stop_profit and not is_lost and self.stop_lost > profit:
                continue

            if self.stop_time > 0:
                pass
