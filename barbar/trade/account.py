import log
import uuid


class Account:
    def __init__(self, repo):
        self.log = log.get_logger(self.__class__.__name__)
        self.repo = repo
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

    def on_quot(self, payload):
        for code in self.position.keys():
            if code in payload:
                self._update_position_quot(code, payload[code])
        if self.cost > 0:
            self.profit_rate = self.profit / self.cost



