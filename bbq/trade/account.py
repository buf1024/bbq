import bbq.log as log
import uuid
from bbq.data.stockdb import StockDB
from bbq.trade.broker.broker import Broker


class Account:
    def __init__(self, account_id: str, db: StockDB):
        self.log = log.get_logger(self.__class__.__name__)

        self.account_id = account_id
        self.db = db

        self.cash_init = 0
        self.cash_available = 0

        self.cost = 0
        self.profit = 0
        self.profit_rate = 0

        self.position = {}
        self.entrust = {}

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
