from typing import Optional

import pandas as pd
from tqdm import tqdm
from bbq.data.stockdb import StockDB
from bbq.selector.strategy.strategy import Strategy


class HorizontalPrice(Strategy):
    def __init__(self, db, *, test_end_date=None, select_count=999999):
        super().__init__(db, test_end_date=test_end_date, select_count=select_count)
        self.min_trade_days = 60
        self.min_break_days = 3
        self.min_break_up = 5.0
        self.max_break_con_up = 3.0
        self.min_horizon_days = 30
        self.max_horizon_con_shock = 3.0
        self.max_horizon_shock = 15.0
        self.sort_by = None

    @staticmethod
    def desc():
        return '  名称: 底部横盘突破选股(基于日线)\n' + \
               '  说明: 选择底部横盘的股票\n' + \
               '  参数: min_trade_days -- 最小上市天数(默认: 60)\n' + \
               '        min_break_days -- 最近突破上涨天数(默认: 3)\n' + \
               '        min_break_up -- 最近累计突破上涨百分比(默认: 5.0)\n' + \
               '        max_break_con_up -- 最近突破上涨百分比(默认: 3.0)\n' + \
               '        min_horizon_days -- 最小横盘天数(默认: 30)\n' + \
               '        max_horizon_con_shock -- 横盘天数内隔天波动百分比(默认: 3.0)\n' + \
               '        max_horizon_shock -- 横盘天数内总波动百分比(默认: 10.0)\n' + \
               '        sort_by -- 排序(默认: None, close -- 现价, rise -- 阶段涨幅)'

    async def prepare(self, **kwargs):
        """
        初始化接口
        :param kwargs:
        :return: True/False
        """
        await super().prepare(**kwargs)
        try:
            if kwargs is not None and 'min_trade_days' in kwargs:
                self.min_trade_days = int(kwargs['min_trade_days'])
            if kwargs is not None and 'min_break_days' in kwargs:
                self.min_break_days = int(kwargs['min_break_days'])
            if kwargs is not None and 'min_break_up' in kwargs:
                self.min_break_up = float(kwargs['min_break_up'])
            if kwargs is not None and 'max_break_con_up' in kwargs:
                self.max_break_con_up = float(kwargs['max_break_con_up'])
            if kwargs is not None and 'min_horizon_days' in kwargs:
                self.min_horizon_days = int(kwargs['min_horizon_days'])
                if self.min_trade_days <= 0 or self.min_horizon_days > self.min_trade_days:
                    self.log.error('策略参数min_horizon_days不合法: {}~{}'.format(0, self.min_trade_days))
                    return False
            if kwargs is not None and 'max_horizon_con_shock' in kwargs:
                self.max_horizon_con_shock = float(kwargs['max_horizon_con_shock'])
            if kwargs is not None and 'max_horizon_shock' in kwargs:
                self.max_horizon_shock = float(kwargs['max_horizon_shock'])

            if kwargs is not None and 'sort_by' in kwargs:
                self.sort_by = kwargs['sort_by']
                if self.sort_by.lower() not in ('close', 'rise'):
                    self.log.error('sort_by不合法')
                    return False

        except ValueError:
            self.log.error('策略参数不合法')
            return False
        self.is_prepared = True
        return self.is_prepared

    async def select(self):
        """
        根据策略，选择股票
        :return: code, name,
        """
        load_info_func = self.db.load_stock_info
        if not isinstance(self.db, StockDB):
            load_info_func = self.db.load_fund_info
        codes = await load_info_func(projection=['code', 'name'])

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

    async def test(self, code: str, name: str = None) -> Optional[pd.DataFrame]:

        load_daily_func, load_info_func = self.db.load_stock_daily, self.db.load_stock_info
        if not isinstance(self.db, StockDB):
            load_daily_func, load_info_func = self.db.load_fund_daily, self.db.load_fund_info

        kdata = await load_daily_func(filter={'code': code,
                                              'trade_date': {'$lte': self.test_end_date}},
                                      limit=self.min_trade_days,
                                      sort=[('trade_date', -1)])

        if kdata is None or kdata.shape[0] < self.min_break_days + self.min_horizon_days:
            return None

        kdata = kdata[::-1]
        kdata['diff'] = kdata['close'].diff()
        kdata['diff'] = kdata['diff'].fillna(value=0.0)
        kdata['rise'] = (kdata['diff'] * 100) / (kdata['close'] - kdata['diff'])
        kdata['rise'] = kdata['rise'].apply(lambda x: round(x, 2))
        kdata = kdata[::-1]

        test_data = kdata[:self.min_break_days]
        break_rise = test_data['rise'].sum()
        if break_rise < self.min_break_up:
            return None

        fit_days = 0
        for df in test_data.to_dict('records'):
            rise = abs(df['rise'])
            if rise <= self.max_break_con_up:
                fit_days = fit_days + 1
                continue
            break
        if fit_days < self.min_break_days:
            return None

        test_data = kdata[self.min_break_days:]

        fit_days = 0
        for df in test_data.to_dict('records'):
            rise = abs(df['rise'])
            if rise <= self.max_horizon_con_shock:
                fit_days = fit_days + 1
                continue
            break

        if fit_days < self.min_horizon_days:
            return None

        hor_close, pre_hor_close = test_data.iloc[0]['close'], kdata.iloc[self.min_horizon_days]['close']
        rise = round((hor_close - pre_hor_close) * 100 / pre_hor_close, 2)
        if rise > self.max_horizon_shock:
            return None

        got_data = dict(code=code, name=name,
                        close=kdata.iloc[0]['close'], break_rise=break_rise,
                        fit_days=fit_days, horizon_rise=rise)
        return pd.DataFrame([got_data])


if __name__ == '__main__':
    from bbq import *

    fund, stock, mysql = default(log_level='error')
    s = HorizontalPrice(db=stock, test_end_date='20211213')


    async def tt():
        await s.plot('sh600021')


    run_until_complete(tt())
