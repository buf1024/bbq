import bbq.log as log
import pandas as pd
import numpy as np
from typing import Optional
from bbq.data.stockdb import StockDB
from datetime import datetime, timedelta
from bbq.analyse.plot import my_plot
from bbq.fetch.my_trade_date import is_trade_date
from tqdm import tqdm
from bbq.analyse.plot import up_color


class Strategy:
    def __init__(self, db, *, test_end_date=None, select_count=999999):
        """

        :param db: stock/fund/mysql db
        :param test_end_date: 测试截止交易日，None为数据库中日期
        :param select_count: 默认选择最大个数
        """
        self.log = log.get_logger(self.__class__.__name__)
        self.db = db
        self.test_end_date = test_end_date
        now = datetime.now()
        if self.test_end_date is None:
            self.test_end_date = datetime(year=now.year, month=now.month, day=now.day)
        else:
            ex = False
            for fmt in ['%Y-%m-%d', '%Y%m%d']:
                ex = False
                try:
                    self.test_end_date = datetime.strptime(self.test_end_date, fmt)
                    break
                except ValueError:
                    ex = True
            if ex:
                self.test_end_date = datetime(year=now.year, month=now.month, day=now.day)
        self.select_count = select_count

    @staticmethod
    def desc():
        pass

    async def prepare(self, **kwargs):
        """
        初始化接口
        :param kwargs:
        :return: True/False
        """
        def_count = self.select_count
        def_test_end_date = self.test_end_date
        try:
            if kwargs is not None and 'select_count' in kwargs:
                self.select_count = int(kwargs['select_count'])
            if kwargs is not None and 'test_end_date' in kwargs:
                self.test_end_date = kwargs['test_end_date']
                ex = False
                for fmt in ['%Y-%m-%d', '%Y%m%d']:
                    ex = False
                    try:
                        self.test_end_date = datetime.strptime(self.test_end_date, fmt)
                        break
                    except ValueError:
                        ex = True
                if ex:
                    self.test_end_date = def_test_end_date
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

    async def backtest(self, **kwargs) -> Optional[pd.DataFrame]:
        if not await self.prepare(**kwargs):
            self.log.error('策略 {} 初始化失败'.format(self.__class__.__name__))
            return None

        now = datetime.now()
        now = datetime(year=now.year, month=now.month, day=now.day)

        if now <= self.test_end_date:
            self.log.error('回测策略test_end_date: {}, 必须大于当天'.format(self.test_end_date))
            return None

        data = await self.select()
        if data is not None and not data.empty:
            if len(data) > self.select_count:
                data = data[:self.select_count]
        await self.destroy()

        if data is None or data.empty:
            return data

        rise_dict = {'1day': None, '3day': None, '5day': None, '10day': None, 'now': None}
        trade_days = 0
        test_date = self.test_end_date + timedelta(days=1)
        while test_date <= now:
            if is_trade_date(test_date):
                trade_days = trade_days + 1
                if trade_days == 1:
                    rise_dict['1day'] = test_date
                if trade_days == 3:
                    rise_dict['3day'] = test_date
                if trade_days == 5:
                    rise_dict['5day'] = test_date
                if trade_days == 10:
                    rise_dict['10day'] = test_date
            test_date = test_date + timedelta(days=1)
        rise_dict['latest'] = now

        db_load_func = self.db.load_stock_daily if isinstance(self.db, StockDB) else self.db.load_fund_daily

        add_list = []
        proc_bar = tqdm(data.to_dict('records'))
        for item in proc_bar:
            proc_bar.set_description('backtest 处理 {}'.format(item['code']))
            add_dict = {'code': item['code']}
            for key, val in rise_dict.items():
                if val is not None:
                    kdata = await db_load_func(filter={'code': item['code'],
                                                       'trade_date': {'$gte': self.test_end_date, '$lte': val}},
                                               sort=[('trade_date', -1)])
                    if kdata is None:
                        add_dict[key] = np.nan
                    elif key != 'latest' and kdata.iloc[0]['trade_date'] != val:
                        add_dict[key] = np.nan
                    else:
                        close, pre_close = kdata.iloc[0]['close'], kdata.iloc[-1]['close']
                        rise = round((close - pre_close)*100/pre_close, 2)
                        add_dict[key] = rise
            add_list.append(add_dict)
        proc_bar.close()
        add_df = pd.DataFrame(add_list)
        data = data.merge(add_df, on='code')
        return data

    async def plot_data(self, code, limit, skip_test_end_date=True):

        flter = {'code': code}
        if self.test_end_date is not None and not skip_test_end_date:
            flter = {'code': code, 'trade_date': {'$lte': self.test_end_date}}
        load_fun = self.db.load_stock_daily if isinstance(self.db, StockDB) else self.db.load_fund_daily
        data = await load_fun(filter=flter, limit=limit, sort=[('trade_date', -1)])
        if data is not None and not data.empty:
            data = data[::-1]
        return data

    async def plot(self, code, limit=60, marks=None):
        """
        plot图象观察
        :param marks: [{color:xx, data:[{trade_date:.. tip:...}...]}]
        :param code:
        :param limit: k线数量
        :return:
        """
        data = await self.plot_data(code, limit)
        if data is None or data.empty:
            return None

        if marks is None and self.test_end_date is not None:
            marks = []

        if self.test_end_date is not None:
            trade_date = self.test_end_date
            while not is_trade_date(trade_date):
                trade_date = trade_date + timedelta(days=-1)

            marks.append({'color': up_color(),
                          'symbol': 'diamond',
                          'data': [{'trade_date': trade_date,
                                    'tip': '测试日期(test_end_date)，观察后面涨幅'}]})

        return my_plot(data=data, marks=marks)
