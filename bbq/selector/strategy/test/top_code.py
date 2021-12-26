from typing import Optional

from bbq.selector.strategy.strategy import Strategy
from bbq.data.stockdb import StockDB
from bbq.analyse.tools import linear_fitting
import pandas as pd
from tqdm import tqdm


class TopCode(Strategy):
    def __init__(self, db, *, test_end_date=None, select_count=999999):
        super().__init__(db, test_end_date=test_end_date, select_count=select_count)
        self.days = 30
        self.min_days = 10
        self.coef = None
        self.score = None
        self.sort_by = None

    @staticmethod
    def desc():
        return '  名称: 龙头选股策略(基于日线)\n' + \
               '  说明: 选择上涨趋势的选股\n' + \
               '  参数: days -- 最近交易天数(默认: 30)\n' + \
               '        min_days -- 最小上市天数(默认: 10)\n' + \
               '        coef -- 线性拟合系数(默认: None)\n' + \
               '        score -- 线性拟合度(默认: None)\n' + \
               '        sort_by -- 结果排序字段(默认: None, 即: score)'

    async def prepare(self, **kwargs):
        """
        初始化接口
        :param kwargs:
        :return: True/False
        """
        await super(TopCode, self).prepare(**kwargs)
        self.days = kwargs['days'] if kwargs is not None and 'days' in kwargs else 30
        self.min_days = kwargs['min_days'] if kwargs is not None and 'min_days' in kwargs else 10
        self.coef = kwargs['coef'] if kwargs is not None and 'coef' in kwargs else None
        self.score = kwargs['score'] if kwargs is not None and 'score' in kwargs else None
        self.sort_by = kwargs['sort_by'] if kwargs is not None and 'sort_by' in kwargs else None

        try:
            self.days = int(self.days)
            self.min_days = int(self.min_days)
            if self.coef is not None:
                self.coef = float(self.coef)
            if self.score is not None:
                self.score = float(self.score)
            if self.sort_by is not None:
                if self.sort_by.lower() not in ('coef', 'score'):
                    self.log.error('sort_by不合法')
                    return False
            else:
                self.sort_by = 'score'

        except ValueError:
            self.log.error('策略参数不合法')
            return False
        self.is_prepared = True
        return self.is_prepared

    async def test(self, code: str, name: str = None) -> Optional[pd.DataFrame]:
        kdata = await self.load_kdata(filter={'code': code,
                                              'trade_date': {'$lte': self.test_end_date}},
                                      limit=self.days,
                                      sort=[('trade_date', -1)])
        if kdata is None or kdata.shape[0] < self.min_days:
            return None
        kdata = kdata[::-1]

        rise = round((kdata.iloc[-1]['close'] - kdata.iloc[0]['close']) * 100 / kdata.iloc[0]['close'], 2)
        a, b, score, x_index, y_index = linear_fitting(kdata)
        if a is None or b is None or x_index is None or y_index is None or score is None:
            return None
        a, b, score = round(a, 4), round(b, 4), round(score, 4)

        name = await self.code_name(code=code, name=name)
        df = None
        if self.coef is not None and self.score is not None:
            if a > self.coef and score > self.score:
                df = pd.DataFrame([dict(code=code, name=name, coef=a, score=score, rise=rise / 100)])
        else:
            df = pd.DataFrame([dict(code=code, name=name, coef=a, score=score, rise=rise / 100)])
        return df
