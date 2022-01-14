import bbq.log as log
import pandas as pd
import numpy as np
from typing import Optional
from bbq.data.stockdb import StockDB
from datetime import datetime, timedelta
from bbq.analyse.plot import my_plot
from bbq.fetch.my_trade_date import is_trade_date
from bbq.analyse.plot import up_color
from tqdm import tqdm
import os


class Strategy:
    def __init__(self, db, *,
                 test_end_date=None, select_count=999999, min_trade_days=60, skip_kcb=True):
        """

        :param db: stock/fund/mysql db
        :param test_end_date: 测试截止交易日，None为数据库中日期
        :param select_count: 默认选择最大个数
        :param min_trade_days 最小交易天数
        :skip_kcb 是否忽略科创板股票
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
        self.skip_kcb = skip_kcb
        self.sort_by = None
        self.min_trade_days = min_trade_days
        self.is_prepared = False

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
            if kwargs is not None and 'min_trade_days' in kwargs:
                self.min_trade_days = int(kwargs['min_trade_days'])

            if kwargs is not None and 'skip_kcb' in kwargs:
                self.skip_kcb = kwargs['test_end_date']

            if kwargs is not None and 'sort_by' in kwargs:
                self.sort_by = kwargs['sort_by']
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
        :return: code, name 必须返回的, 1day, 3day, 5day, 10day, latest的涨幅，如果有尽量返回
            [{code, name...}, {code, name}, ...]/None
        """
        load_info_func = self.db.load_stock_info
        if not isinstance(self.db, StockDB):
            load_info_func = self.db.load_fund_info
        codes = await load_info_func(projection=['code', 'name'])
        return await self.do_select(codes=codes)

    async def test(self, code: str, name: str = None) -> Optional[pd.DataFrame]:
        """
        根据策略，测试股票是否符合策略
        :param code
        :param name
        :return: 符合策略的，返回和select 一样的结构
            code, name 必须返回的, 1day, 3day, 5day, 10day, latest的涨幅，如果有尽量返回
            [{code, name...}, {code, name}, ...]/None
        """
        raise Exception('选股策略 {} 没实现测试函数'.format(self.__class__.__name__))

    async def backtest(self, code: str, name: str = None, with_stat=True, **kwargs) -> Optional[pd.DataFrame]:
        if not self.is_prepared and not await self.prepare(**kwargs):
            self.log.error('策略 {} 初始化失败'.format(self.__class__.__name__))
            return None

        data = await self.test(code=code, name=name)
        if data is None or data.empty:
            return data

        if with_stat:
            data = await self.stat_data(data=data)
        return data

    async def run(self, with_stat=True, **kwargs) -> Optional[pd.DataFrame]:
        if not self.is_prepared and not await self.prepare(**kwargs):
            self.log.error('策略 {} 初始化失败'.format(self.__class__.__name__))
            return None

        data = await self.select()
        if data is not None and not data.empty:
            if len(data) > self.select_count:
                data = data[:self.select_count]

        if data is None or data.empty:
            return data

        if with_stat:
            data = await self.stat_data(data=data)
        return data

    async def plot(self, code, *, limit=60, skip_test_end_date=True, s_data=None, marks=None):
        """
        plot图象观察
        :param s_data dict
        :param marks: [{color:xx, data:[{trade_date:.. tip:...}...]}]
        :param code:
        :param limit: k线数量
        :param skip_test_end_date
        :return:
        """
        data = await self.plot_data(code, limit, skip_test_end_date)
        if data is None or data.empty:
            return None

        if marks is None and self.test_end_date is not None:
            marks = []

        if self.test_end_date is not None:
            trade_date = self.test_end_date
            while not is_trade_date(trade_date):
                trade_date = trade_date + timedelta(days=-1)

            tip = ''
            if s_data is not None:
                keys = ['1day', '3day', '5day', '10day', 'latest']
                for key, val in s_data.items():
                    if key in keys and val is not None and not np.isnan(val):
                        tmp_tip = '{} 涨幅:&nbsp;&nbsp;{}%'.format(key, val)
                        if len(tip) == 0:
                            tip = tmp_tip
                            continue
                        tip = '{}\n{}'.format(tip, tmp_tip)

            marks.append({'color': up_color(),
                          'symbol': 'diamond',
                          'data': [{'trade_date': trade_date,
                                    'tip': '留意后面涨幅\n{}'.format(tip)}]})

        return my_plot(data=data, marks=marks)

    async def plots(self, data, limit=60, skip_test_end_date=True):
        if data is None:
            return None

        charts = {}
        items = data.to_dict('records')
        for item in items:
            chart = await self.plot(code=item['code'], limit=limit, skip_test_end_date=skip_test_end_date, s_data=item)
            if chart is not None:
                charts[item['code']] = chart
        return charts

    async def plots_to_path(self, path, data, limit=60):
        charts = await self.plots(data=data, limit=limit)
        if charts is None:
            return
        os.makedirs(path, exist_ok=True)
        for k, v in charts.items():
            file_path = os.sep.join([path, k + '.html'])
            v.render(file_path)

    @staticmethod
    def export_to_path(path, data):
        codes = data['code'].to_list()
        cont = '\n'.join(codes)
        with open(path, mode='w') as f:
            f.write(cont)

    # 以下为保护方法，外面不要调用
    # 不加下划线变为protected是因为，加了之后编辑器无法提示
    async def code_name(self, code, name=None):
        if name is not None:
            return name
        load_daily_func, load_info_func = self.db.load_stock_daily, self.db.load_stock_info
        if not isinstance(self.db, StockDB):
            load_daily_func, load_info_func = self.db.load_fund_daily, self.db.load_fund_info
        name_df = await load_info_func(filter={'code': code}, limit=1)
        if name_df is not None and not name_df.empty:
            name = name_df.iloc[0]['name']
        return name

    async def load_kdata(self, with_rise=True, **kwargs):
        load_daily_func, load_info_func = self.db.load_stock_daily, self.db.load_stock_info
        if not isinstance(self.db, StockDB):
            load_daily_func, load_info_func = self.db.load_fund_daily, self.db.load_fund_info
        kdata = await load_daily_func(**kwargs)

        if kdata is not None and with_rise:
            # test_data = kdata[::]
            test_data = kdata
            test_data = test_data.sort_values(by='trade_date')
            test_data['diff'] = test_data['close'].diff()
            test_data['diff'] = test_data['diff'].fillna(value=0.0)
            test_data['rise'] = (test_data['diff'] * 100) / (test_data['close'] - test_data['diff'])
            test_data['rise'] = test_data['rise'].apply(lambda x: round(x, 2))
            test_data = test_data[['trade_date', 'diff', 'rise']]
            kdata = kdata.merge(test_data, on='trade_date')

        return kdata

    @staticmethod
    def is_long_leg(df, ratio, side=None) -> bool:
        close, high, low, open_ = df['close'], df['high'], df['low'], df['open']
        if high == low:
            return False
        if side is None:
            side = ['top', 'bottom']
        if isinstance(side, str):
            side = [side]
        for s in side:
            if s == 'top':
                r = (high - close) * 100 / (high - low)
                if r > ratio:
                    return True
            if s == 'bottom':
                r = (open_ - low) * 100 / (high - low)
                if r > ratio:
                    return True

        return False

    @staticmethod
    def is_short_leg(df, ratio, side=None) -> bool:
        return not Strategy.is_long_leg(df=df, ratio=ratio, side=side)

    async def stock_concept(self, code):
        if isinstance(self.db, StockDB):
            df = await self.db.load_stock_concept(filter={'stock_code': code},
                                                  sort=[('concept_date', -1)])
            if df is None or df.empty:
                df = await self.db.load_stock_concept(filter={'stock_name': code},
                                                      sort=[('concept_date', -1)])
            return df
        return None

    async def stock_concept_str(self, code):
        df = await self.stock_concept(code=code)
        if df is not None and not df.empty:
            s = []
            lst = list(df['concept_name'])
            while len(lst) > 0:
                tmp_lst = lst[:3]
                s.append(','.join(tmp_lst))

                lst = lst[3:]
            return '\n'.join(s)
        return ''

    async def stat_data(self, data):
        now = datetime.now()
        now = datetime(year=now.year, month=now.month, day=now.day)

        rise_dict = {'1day': None, '2day': None, '3day': None, '4day': None, '5day': None, '10day': None, 'now': None}
        trade_days = 0
        test_date = self.test_end_date + timedelta(days=1)
        while test_date <= now:
            if is_trade_date(test_date):
                trade_days = trade_days + 1
                if trade_days == 1:
                    rise_dict['1day'] = test_date
                if trade_days == 2:
                    rise_dict['2day'] = test_date
                if trade_days == 3:
                    rise_dict['3day'] = test_date
                if trade_days == 4:
                    rise_dict['4day'] = test_date
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
            proc_bar.set_description('统计 {}'.format(item['code']))

            add_dict = {'code': item['code']}
            for key, val in rise_dict.items():
                if val is not None:
                    kdata = await db_load_func(filter={'code': item['code'],
                                                       'trade_date': {'$gte': self.test_end_date, '$lte': val}},
                                               sort=[('trade_date', -1)])
                    if kdata is None:
                        # 可能停牌
                        add_dict[key] = 0
                    else:
                        if len(kdata) < 2:
                            add_dict[key] = 0
                        else:
                            close, pre_close = kdata.iloc[0]['close'], kdata.iloc[-1]['close']
                            # close, pre_close = kdata.iloc[0]['close'], kdata.iloc[1]['close']
                            rise = round((close - pre_close) * 100 / pre_close, 2)
                            add_dict[key] = rise

            if isinstance(self.db, StockDB):
                add_dict['concept'] = await self.stock_concept_str(code=item['code'])
            add_list.append(add_dict)
        proc_bar.close()
        add_df = pd.DataFrame(add_list)
        data = data.merge(add_df, on='code')
        return data

    async def plot_data(self, code, limit, skip_test_end_date=True):
        flter = {'code': code}
        if self.test_end_date is not None and not skip_test_end_date:
            flter = {'code': code, 'trade_date': {'$lte': self.test_end_date}}
        load_fun, load_info = (self.db.load_stock_daily, self.db.load_stock_info) if isinstance(self.db, StockDB) \
            else (self.db.load_fund_daily, self.db.load_fund_info)

        data = await load_fun(filter=flter, limit=limit, sort=[('trade_date', -1)])
        if data is not None and not data.empty:
            name_df = await load_info(filter={'code': code}, projection=['code', 'name'])
            if name_df is not None and not data.empty:
                data = data.merge(name_df, on='code')
            data = data[::-1]
        return data

    async def do_select(self, codes: pd.DataFrame) -> Optional[pd.DataFrame]:
        if codes is None:
            return None
        select = []
        proc_bar = tqdm(codes.to_dict('records'))
        for item in proc_bar:
            if 'ST' in item['name'].upper():
                continue
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
            if self.sort_by is not None:
                select = sorted(select, key=lambda v: v[self.sort_by], reverse=True)
            df = pd.DataFrame(select)

        return df
