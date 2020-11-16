import random
from selector.strategy.strategy import Strategy


class Random(Strategy):
    def __init__(self):
        super().__init__()
        self.count = 10
        self.market = None
        self.kwargs = None

    def init(self, db=None, quot=None, **kwargs):
        super().init(db=db, quot=None)
        self.kwargs = kwargs
        if kwargs is not None:
            if 'count' in kwargs:
                try:
                    count = int(kwargs['count'])
                    if count <= 0:
                        self.log.error('策略参数 count={} 不为正整数'.format(kwargs['count']))
                        return False
                    self.count = count
                except ValueError:
                    self.log.error('策略参数 count={} 不为整数'.format(kwargs['count']))
                    return False

            if 'market' in kwargs:
                market = kwargs['market'].upper()
                if market != 'SZ' and market != 'SH':
                    self.log.error('策略参数 market={} 不为 SZ / SH'.format(kwargs['market']))
                    return False
                self.market = market
        return True

    def name(self):
        return 'random'

    def desc(self):
        return '名称: 随机策略\n' + \
               '说明: 随机选股测试策略\n' + \
               '参数: count  -- 选择个数(默认10)\n' + \
               '      market -- 选择市场(值为: SZ 或 SH, 无默认值)'

    def select(self, **kwargs):
        """
        根据策略，选择股票
        :param kwargs:
        :return: [(code, info), (code, info)...]/None
        """
        flter = {'code': {'$regex': self.market + '$'}} if self.market is not None else None
        codes = self.db.load_code_list(filter=flter, projection=['code'])
        if codes is None:
            return []

        choice = codes['code'].tolist()
        return random.sample(choice, self.count)

    def regression(self, codes=None, **kwargs):
        """
        根据策略，对股票进行回归
        :param codes:
        :param kwargs:
        :return:
        """
        return 1.0
