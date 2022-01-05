from typing import Optional
import pandas as pd
from bbq.selector.strategy.strategy import Strategy


class RiseShock(Strategy):
    """
    右侧上涨后，开始震荡
    示意形态:
       | | | |
      |
     |
    |
    """
    def __init__(self, db, *, test_end_date=None):
        super().__init__(db, test_end_date=test_end_date)
        self.max_shock_days = 15
        self.max_shock_day_ratio = 3.5
        self.max_shock_ratio = 3.5
        self.min_rise_last_day = 4.0
        self.is_con_rise = True
        self.min_rise_days = 3
        self.min_acct_rise = 9.0
        self.sort_by = None

    @staticmethod
    def desc():
        return '  名称: 爬升震荡(基于日线)\n' + \
               '  说明: 右侧上涨爬升震荡的股票\n' + \
               '  参数: max_shock_days -- 水平震荡的最大天数(默认: 15)\n' + \
               '        max_shock_day_ratio -- 单日水平震荡的最大百分比(默认: 3.5)\n' + \
               '        max_shock_ratio -- 水平震荡期间最后一天和当天的最大涨跌幅百分比(默认: 3.5)\n' + \
               '        min_rise_last_day -- 震荡开始前一日最少上涨百分比(默认: 4.0)\n' + \
               '        is_con_rise -- 震荡开始前最后一日涨幅是否大于前一天(默认: True)\n' + \
               '        min_rise_days -- 震荡开始前连续上涨天数(默认: 3)\n' + \
               '        min_acct_rise -- 震荡开始前最少累计上涨百分比(默认: 9.0)'

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
            if kwargs is not None and 'max_shock_days' in kwargs:
                self.max_shock_days = int(kwargs['max_shock_days'])
            if kwargs is not None and 'max_shock_day_ratio' in kwargs:
                self.max_shock_day_ratio = float(kwargs['max_shock_day_ratio'])
            if kwargs is not None and 'max_shock_ratio' in kwargs:
                self.max_shock_ratio = float(kwargs['max_shock_ratio'])
            if kwargs is not None and 'min_rise_last_day' in kwargs:
                self.min_rise_last_day = float(kwargs['min_rise_last_day'])
            if kwargs is not None and 'is_con_rise' in kwargs:
                self.is_con_rise = bool(kwargs['is_con_rise'])
            if kwargs is not None and 'min_rise_days' in kwargs:
                self.min_rise_days = int(kwargs['min_rise_days'])
            if kwargs is not None and 'min_acct_rise' in kwargs:
                self.min_acct_rise = float(kwargs['min_acct_rise'])

        except ValueError:
            self.log.error('策略参数不合法')
            return False
        self.is_prepared = True
        return self.is_prepared

    async def test(self, code: str, name: str = None) -> Optional[pd.DataFrame]:
        if self.skip_kcb and code.startswith('sh688'):
            return None

        kdata = await self.load_kdata(filter={'code': code,
                                              'trade_date': {'$lte': self.test_end_date}},
                                      limit=self.min_trade_days,
                                      sort=[('trade_date', -1)])

        if kdata is None or kdata.shape[0] < self.min_trade_days:
            return None

        fit_days = 0
        fit_start_index = -1
        shock_rise = 0
        pre_rise = 0
        for i in range(kdata.shape[0]):
            df = kdata.iloc[i]
            rise = df['rise']
            if abs(rise) <= self.max_shock_day_ratio:
                fit_days = fit_days + 1
                continue
            if fit_days > 0 and fit_start_index == -1:
                if rise >= self.min_rise_last_day:
                    fit_start_index = i
                    pre_rise = rise
                    shock_rise = abs(round((df['close'] - kdata.iloc[0]['close'])*100/kdata.iloc[0]['close'], 2))
            break

        if fit_start_index == -1 or fit_days > self.max_shock_days or shock_rise > self.max_shock_ratio:
            return None

        test_data = kdata[fit_days:]
        cont_rise_days = 0
        acct_rise = 0
        for i in range(test_data.shape[0]):
            rise = test_data.iloc[i]['rise']
            if rise <= 0:
                break
            if self.is_con_rise and rise > pre_rise:
                break
            cont_rise_days = cont_rise_days + 1
            acct_rise = acct_rise + rise
            pre_rise = rise

        if cont_rise_days < self.min_rise_days or acct_rise < self.min_acct_rise:
            return None

        name = await self.code_name(code=code, name=name)
        got_data = dict(code=code, name=name,
                        close=kdata.iloc[0]['close'], shock_start=kdata.iloc[fit_days]['trade_date'],
                        shock_days=fit_days, shock_rise=shock_rise,
                        rise_days=cont_rise_days, acct_rise=acct_rise)
        return pd.DataFrame([got_data])


if __name__ == '__main__':
    from bbq import *

    fund, stock, mysql = default(log_level='error')
    s = RiseShock(db=stock, test_end_date='20211227')


    async def tt():
        df = await s.test(code='sz300344')
        print(df)


    run_until_complete(tt())
