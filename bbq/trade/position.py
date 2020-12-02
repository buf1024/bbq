from bbq.trade.account import Account
from bbq.trade.base_obj import BaseObj
from datetime import datetime, timedelta
import uuid


class Position(BaseObj):
    def __init__(self, position_id: str, account: Account):
        super().__init__(typ=account.typ, db_data=account.db_data, db_trade=account.db_trade)
        self.account = account

        self.position_id = position_id

        self.name = ''  # 股票名称
        self.code = ''  # 股票代码
        self.time = datetime.now()  # 首次建仓时间

        self.volume = 0  # 持仓量
        self.volume_available = 0  # 可用持仓量

        self.cost = 0.0  # 平均持仓成本
        self.price = 0.0  # 平均持仓价

        self.now_price = 0.0  # 最新价
        self.max_price = 0.0  # 最高价
        self.min_price = 0.0  # 最低价

        self.profit_rate = 0.0  # 盈利比例
        self.max_profit_rate = 0.0  # 最大盈利比例
        self.min_profit_rate = 0.0  # 最小盈利比例

        self.profit = 0.0  # 盈利
        self.max_profit = 0.0  # 最大盈利
        self.min_profit = 0.0  # 最小盈利

        self.max_profit_time = None  # 最大盈利时间
        self.min_profit_time = None  # 最小盈利时间

    # def _get_adj_factor(self, codes, trade_date, pre_close):
    #     if trade_date in self._adj_check_date:
    #         return self.adj_factor
    #
    #     price = self.repo.db.load_code_kdata(codes=codes,
    #                                          filter={'trade_date': {'$lt': trade_date}},
    #                                          projection=['close'],
    #                                          limit=1)
    #     if price is None or price.empty:
    #         return self.adj_factor
    #
    #     db_pre_close = price['close'].tolist()[0]
    #     if db_pre_close == pre_close:
    #         return self.adj_factor
    #
    #     self.adj_factor = self.adj_factor * (pre_close / db_pre_close)
    #
    #     return self.adj_factor

    def on_quot(self, quot):
        # adj_factor = self._get_adj_factor(codes=[self.code], trade_date=quot['trade_date'], pre_close=quot['pre_close'])

        adj_factor = 1.0
        self.volume = self.volume * adj_factor
        self.price = quot.now * adj_factor

        self.max_price = self.price if self.max_price < self.price else self.max_price
        self.min_price = self.price if self.min_price > self.price else self.min_price

        self.profit = (self.price - self.cost) * self.volume
        self.max_profit = self.profit if self.profit > self.max_profit else self.max_profit

        self.profit_rate = self.profit / (self.cost * self.volume)
        self.max_profit_rate = self.profit if self.profit_rate > self.max_profit_rate else self.max_profit_rate

    async def sync_from_db(self) -> bool:
        position = await self.db_trade.load_strategy(filter={'position_id': self.position_id}, limit=1)
        position = None if len(position) == 0 else position[0]
        if position is None:
            self.log.error('position from db not found: {}'.format(self.position_id))
            return False
        # todo
        return True

    @BaseObj.discard_saver
    async def sync_to_db(self) -> bool:
        # data = {'account_id': self.account.account_id,
        #         'strategy_id': self.strategy_id,
        #         'strategy_opt': json.dumps(self.opt) if self.opt is not None else None}
        # await self.db_trade.save_strategy(data=data)
        return True