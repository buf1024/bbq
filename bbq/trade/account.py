import bbq.log as log
from bbq.data.mongodb import MongoDB
from bbq.trade.tradedb import TradeDB
from bbq.trade.base_obj import BaseObj


class Account(BaseObj):
    def __init__(self, account_id: str, typ: str, db_data: MongoDB, db_trade: TradeDB):
        super().__init__(typ=typ, db_data=db_data, db_trade=db_trade)

        self.account_id = account_id

        self.cash_init = 0
        self.cash_available = 0

        self.cost = 0
        self.profit = 0
        self.profit_rate = 0

        self.position = {}
        self.entrust = {}

    async def sync_from_db(self) -> bool:
        pass

    @BaseObj.discard_saver
    async def sync_to_db(self) -> bool:
        pass

    @property
    def strategy(self):
        return None

    @property
    def broker(self):
        return None

    @property
    def risk(self):
        return None

    @property
    def robot(self):
        return None

    def _update_position_quot(self, code, quot):
        self.position[code].on_quot(quot)
        self.profit += self.position[code].profit
        self.cost += (self.position[code].cost * self.position[code].volume)

    async def on_quot(self, payload):
        for code in self.position.keys():
            if code in payload:
                self._update_position_quot(code, payload[code])
        if self.cost > 0:
            self.profit_rate = self.profit / self.cost

    async def on_broker(self, broker, payload):
        pass

    async def on_risk(self, risk, payload):
        pass

    async def on_strategy(self, strategy, payload):
        pass
