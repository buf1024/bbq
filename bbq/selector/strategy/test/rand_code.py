import random

from bbq.selector.strategy.strategy import Strategy


class RandCode(Strategy):
    def __init__(self, db):
        super().__init__(db)
        self.market = None
        self.db_load_func = None

    async def prepare(self, **kwargs):
        await super().prepare(**kwargs)

        self.market = kwargs['market'] if kwargs is not None and 'market' in kwargs else None

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
               '  参数: select_count  -- 选择个数(默认20)\n' + \
               '        market -- 选择品种(值为: fund 或 stock, 默认stock)'

    async def select(self):
        """
        根据策略，选择股票
        :return: [{code, name...}, {code, name}, ...]/None
        """
        codes = await self.db_load_func(projection=['code', 'name'])
        if codes is None:
            return None

        choice = random.sample(range(len(codes)), self.select_count)
        df = codes.iloc[choice]

        return df.reset_index(drop=True)
