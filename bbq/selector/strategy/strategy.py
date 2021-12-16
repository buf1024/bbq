import bbq.log as log
import pandas as pd
from typing import Optional
from bbq.data.stockdb import StockDB

from bbq.analyse.plot import my_plot


class Strategy:
    def __init__(self, db, *, test_end_date=None, select_count=20):
        """

        :param db: stock/fund/mysql db
        :param test_end_date: 测试截止交易日，None为数据库中日期
        :param select_count: 默认选择最大个数
        """
        self.log = log.get_logger(self.__class__.__name__)
        self.db = db
        self.test_end_date = test_end_date
        self.select_count = select_count

    def desc(self):
        pass

    async def prepare(self, **kwargs):
        """
        初始化接口
        :param kwargs:
        :return: True/False
        """
        def_count = self.select_count
        if kwargs is not None and 'select_count' in kwargs:
            self.select_count = kwargs['select_count']
        try:
            self.select_count = int(self.select_count)
        except ValueError:
            self.select_count = def_count
        return True

    async def destroy(self):
        """
        清理接口
        :return: True/False
        """
        return True

    async def select(self) -> Optional[pd.DataFrame]:
        """
        根据策略，选择股票
        :return: code, name 必须返回的
            [{code, name...}, {code, name}, ...]/None
        """
        raise Exception('选股策略 {} 没实现选股函数'.format(self.__class__.__name__))

    async def run(self, **kwargs) -> Optional[pd.DataFrame]:
        if not await self.prepare(**kwargs):
            self.log.error('策略 {} 初始化失败'.format(self.__class__.__name__))
            return None

        data = await self.select()
        if data is not None and not data.empty:
            if len(data) > self.select_count:
                data = data[:self.select_count]
        await self.destroy()

        return data

    async def plot_data(self, code, limit):
        flter = {'code': code} if self.test_end_date is None else {'code': code,
                                                                   'trade_date': {'$lte': self.test_end_date}}
        load_fun = self.db.load_stock_daily if isinstance(self.db, StockDB) else self.db.load_fund_daily
        return await load_fun(filter=flter, limit=limit, sort=[('trade_date', 1)])

    async def plot(self, code, limit=60, marks=None):
        """
        plot图象观察
        :param marks: [{trade_date:.. value:.., tip:...}...]
        :param code:
        :param limit: k线数量
        :return:
        """
        data = await self.plot_data(code, limit)
        if data is None or data.empty:
            return None

        return my_plot(data=data, marks=marks)
