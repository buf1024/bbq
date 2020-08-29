from datetime import datetime, timedelta


class Position:
    def __init__(self, repo):
        self.repo = repo

        self.name = ''
        self.code = ''

        self.time = None
        self.last_time = None

        self.volume = 0
        self.volume_available = 0

        self.cost = 0.0

        self.price = 0.0
        self.max_price = 0.0
        self.min_price = 0.0

        self.profit_rate = 0.0
        self.max_profit_rate = 0.0

        self.profit = 0.0
        self.max_profit = 0.0

        self.adj_factor = 1.0

        self._adj_check_date = []

    def _get_adj_factor(self, codes, trade_date, pre_close):
        if trade_date in self._adj_check_date:
            return self.adj_factor

        price = self.repo.db.load_code_kdata(codes=codes,
                                             filter={'trade_date': {'$lt': trade_date}},
                                             projection=['close'],
                                             limit=1)
        if price is None or price.empty:
            return self.adj_factor

        db_pre_close = price['close'].tolist()[0]
        if db_pre_close == pre_close:
            return self.adj_factor

        self.adj_factor = self.adj_factor * (pre_close / db_pre_close)

        return self.adj_factor

    def on_quot(self, quot):
        adj_factor = self._get_adj_factor(codes=[self.code], trade_date=quot['trade_date'], pre_close=quot['pre_close'])

        self.volume = self.volume * adj_factor
        self.price = quot.now * adj_factor

        self.max_price = self.price if self.max_price < self.price else self.max_price
        self.min_price = self.price if self.min_price > self.price else self.min_price

        self.profit = (self.price - self.cost) * self.volume
        self.max_profit = self.profit if self.profit > self.max_profit else self.max_profit

        self.profit_rate = self.profit / (self.cost * self.volume)
        self.max_profit_rate = self.profit if self.profit_rate > self.max_profit_rate else self.max_profit_rate
