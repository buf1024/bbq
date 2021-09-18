import random
from bbq.selector.strategy.strategy import Strategy


class RandCode(Strategy):
    def __init__(self, db):
        super().__init__(db)
        self.count = 10
        self.market = None
        self.db_load_func = None

    async def init(self, **kwargs):
        self.count = kwargs['count'] if kwargs is not None and 'count' in kwargs else 10
        self.market = kwargs['market'] if kwargs is not None and 'market' in kwargs else None

        try:
            self.count = int(self.count)
            if self.count <= 0:
                self.log.error('策略参数 count={} 不为正整数'.format(self.count))
                return False
        except ValueError:
            self.log.error('策略参数 count={} 不为整数'.format(self.count))
            return False

        if self.market is not None:
            if self.market != 'fund' and self.market != 'stock':
                self.log.error('策略参数 market={} 不正确, 值为: sz 或 sh'.format(self.market))
                return False

        self.db_load_func = self.db.load_stock_info \
            if self.market == 'stock' or self.market is None \
            else self.db.load_fund_info

        return True

    def desc(self):
        return '  名称: 随机策略\n' + \
               '  说明: 随机选股测试策略\n' + \
               '  参数: count  -- 选择个数(默认10)\n' + \
               '        market -- 选择品种(值为: fund 或 stock, 默认stock)'

    async def select(self):
        """
        根据策略，选择股票
        :return: [{code, ctx...}, {code, ctx}, ...]/None
        """
        codes = await self.db_load_func(projection=['code'])
        if codes is None:
            return None

        choice = random.sample(range(len(codes)), self.count)
        df = codes.iloc[choice]

        return df.reset_index(drop=True)
