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

    async def destroy(self):
        """
        清理接口
        :return: True/False
        """
        return True

    async def select(self):
        """
        根据策略，选择股票
        :return: code, name, coef(系数), score(分数), rise(累计涨幅)
        """
        codes = None
        if isinstance(self.db, StockDB):
            codes = await self.db.load_stock_info(projection=['code', 'name'])
        else:
            codes = await self.db.load_fund_info(projection=['code', 'name'])

        select = []
        proc_bar = tqdm(codes.to_dict('records'))
        for item in proc_bar:
            proc_bar.set_description('处理 {}'.format(item['code']))
            got_data = await self.test(code=item['code'], name=item['name'])
            if got_data is not None:
                select = select + got_data.to_dict('records')
                if len(select) >= self.select_count:
                    proc_bar.update(proc_bar.total)
                    proc_bar.set_description('处理完成select_count={}'.format(self.select_count))
                    self.log.info('select count: {}, break loop'.format(self.select_count))
                    break
        proc_bar.close()

        df = None
        if len(select) > 0:
            select = sorted(select, key=lambda v: v[self.sort_by], reverse=True)
            df = pd.DataFrame(select)

        return df

    async def test(self, code: str, name: str = None) -> Optional[pd.DataFrame]:
        load_daily_func, load_info_func = self.db.load_stock_daily, self.db.load_stock_info
        if not isinstance(self.db, StockDB):
            load_daily_func, load_info_func = self.db.load_fund_daily, self.db.load_fund_info

        kdata = await load_daily_func(filter={'code': code,
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

        if name is None:
            name_df = await load_info_func(filter={'code': code}, limit=1)
            if name_df is not None and not name_df.empty:
                name = name_df.iloc[0]['name']
        df = None
        if self.coef is not None and self.score is not None:
            if a > self.coef and score > self.score:
                df = pd.DataFrame([dict(code=code, name=name, coef=a, score=score, rise=rise / 100)])
        else:
            df = pd.DataFrame([dict(code=code, name=name, coef=a, score=score, rise=rise / 100)])
        return df
