from typing import Optional
import pandas as pd
from bbq.selector.strategy.strategy import Strategy


class ZCode(Strategy):
    """
    横盘震荡，上涨突破，横盘震荡
    示意形态:
           ||||
          |
         |
        |
    ||||
    """
    def __init__(self, db, *, test_end_date=None, select_count=999999):
        super().__init__(db, test_end_date=test_end_date, select_count=select_count)
        self.min_trade_days = 60
        self.right_horizon_days = 15
        self.max_horizon_shock = 3.5
        self.min_rise_up = 4.0
        self.min_rise_days = 3
        self.min_acct_rise = 9.0
        self.left_horizon_days = 6
        self.sort_by = None

    @staticmethod
    def desc():
        return '  名称: Z形态震荡上涨震荡(基于日线)\n' + \
               '  说明: 右侧上涨爬升震荡的股票\n' + \
               '  参数: min_trade_days -- 最小上市天数(默认: 60)\n' + \
               '        right_horizon_days -- 右侧水平震荡的最大天数(默认: 15)\n' + \
               '        max_horizon_shock -- 水平震荡的最大百分比(默认: 3.5)\n' + \
               '        min_rise_up -- 最后一日最小上涨百分比(默认: 4.0)\n' + \
               '        min_rise_days -- 连续上涨天数(默认: 3)\n' + \
               '        min_acct_rise -- 连续上涨百分比(默认: 9.0)\n' + \
               '        left_horizon_days -- 左侧水平震荡的最小天数(默认: 6)\n' + \
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
            if kwargs is not None and 'right_horizon_days' in kwargs:
                self.right_horizon_days = int(kwargs['right_horizon_days'])
            if kwargs is not None and 'max_horizon_shock' in kwargs:
                self.max_horizon_shock = float(kwargs['max_horizon_shock'])
            if kwargs is not None and 'min_rise_up' in kwargs:
                self.min_rise_up = float(kwargs['min_rise_up'])
            if kwargs is not None and 'min_rise_days' in kwargs:
                self.min_rise_days = int(kwargs['min_rise_days'])
            if kwargs is not None and 'min_acct_rise' in kwargs:
                self.min_acct_rise = float(kwargs['min_acct_rise'])
            if kwargs is not None and 'left_horizon_days' in kwargs:
                self.left_horizon_days = int(kwargs['left_horizon_days'])

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

        test_data = kdata
        right_fit_days = 0
        fit_start_index = -1
        for i in range(test_data.shape[0]):
            df = test_data.iloc[i]
            rise = df['rise']
            if abs(rise) <= self.max_horizon_shock:
                right_fit_days = right_fit_days + 1
                continue
            if right_fit_days > 0 and fit_start_index == -1:
                if rise >= self.min_rise_up:
                    fit_start_index = i
            break

        if fit_start_index == -1 or right_fit_days > self.right_horizon_days:
            return None

        test_data = kdata[right_fit_days:]
        cont_rise_days = 0
        acct_rise = 0
        for i in range(test_data.shape[0]):
            rise = test_data.iloc[i]['rise']
            if rise <= 0:
                break
            cont_rise_days = cont_rise_days + 1
            acct_rise = acct_rise + rise

        if cont_rise_days < self.min_rise_days or acct_rise < self.min_acct_rise:
            return None

        test_data = test_data[cont_rise_days:]
        left_fit_days = 0
        for i in range(test_data.shape[0]):
            df = test_data.iloc[i]
            rise = df['rise']
            if abs(rise) <= self.max_horizon_shock:
                left_fit_days = left_fit_days + 1
                continue
            break

        if left_fit_days < self.left_horizon_days:
            return None

        name = await self.code_name(code=code, name=name)
        got_data = dict(code=code, name=name,
                        close=kdata.iloc[0]['close'], right_shock_days=right_fit_days,
                        rise_days=cont_rise_days, left_shock_days=left_fit_days, rise=acct_rise, )
        return pd.DataFrame([got_data])


if __name__ == '__main__':
    from bbq import *

    fund, stock, mysql = default(log_level='error')
    s = ZCode(db=stock, test_end_date='20211224')


    async def tt():
        df = await s.test(code='sh600011')
        await s.plots(df)


    run_until_complete(tt())
