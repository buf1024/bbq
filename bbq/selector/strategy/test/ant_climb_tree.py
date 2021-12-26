import talib
from typing import Optional
import pandas as pd
import numpy as np
from bbq.selector.strategy.strategy import Strategy


class AntClimbTree(Strategy):
    """
    股价经过长期的缩量下跌后，向右下方的34日和55日均线，相距很近或者基本走平.
    13日均线由下跌开始走平，股价踏上13日均线后，以连续上攻的小阳线缓步盘升，把股价轻轻送上55日均线.
    这几根持续上升的小阳线, 我们称之为蚂蚁上树.
    蚂蚁上树形态的k线必须是小阳线，小阳线的数量不能低于五根，否则，蚂蚁上树形态便不能成立。
    不严格，严格基本选不出股票
    """

    def __init__(self, db, *, test_end_date=None, select_count=999999):
        super().__init__(db, test_end_date=test_end_date, select_count=select_count)
        self.min_trade_days = 120
        self.ma_numbers = (5, 10, 20)
        self.ma_numbers_tag = ('ma13', 'ma34', 'ma55')
        self.min_climb_days = 5
        self.max_climb_up = 5.0
        self.ma_same_diff = 3.0
        self.ma_same_days = 15
        self.max_shock_days = 15
        self.sort_by = None

    async def prepare(self, **kwargs):
        await super().prepare(**kwargs)
        try:
            if kwargs is not None and 'min_trade_days' in kwargs:
                self.min_trade_days = int(kwargs['min_trade_days'])
            if kwargs is not None and 'ma_numbers' in kwargs:
                ma_numbers = kwargs['ma_numbers']
                self.ma_numbers = (int(m) for m in ma_numbers.split(','))
                self.ma_numbers_tag = ('ma{}'.format(m) for m in ma_numbers.split(','))
            if kwargs is not None and 'min_climb_days' in kwargs:
                self.min_climb_days = int(kwargs['min_climb_days'])
            if kwargs is not None and 'max_climb_up' in kwargs:
                self.max_climb_up = float(kwargs['max_climb_up'])
            if kwargs is not None and 'ma_same_diff' in kwargs:
                self.ma_same_diff = float(kwargs['ma_same_diff'])
            if kwargs is not None and 'ma_same_days' in kwargs:
                self.ma_same_days = int(kwargs['ma_same_days'])
            if kwargs is not None and 'max_shock_days' in kwargs:
                self.max_shock_days = int(kwargs['max_shock_days'])

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

    @staticmethod
    def desc():
        return '  名称: 蚂蚁上树选股策略\n' + \
               '  说明: 蚂蚁上树形态识别\n' + \
               '  参数: min_trade_days -- 最小上市天数(默认: 120)\n' + \
               '        ma_numbers -- 蚂蚁上树ma选择(默认: 5,10,20， 正常的:13,34,55， 也有10,30,60)\n' + \
               '        max_shock_days -- 蚂蚁上树震荡区最大持续天数(默认: 15)\n' + \
               '        max_shock_ratio -- 蚂蚁上树震荡区下跌ma最大百分比(默认: -2.0)\n' + \
               '        min_climb_days -- 蚂蚁上树爬树天数(默认: 5)\n' + \
               '        max_climb_up -- 蚂蚁上树爬树涨幅(默认: 5.0)\n' + \
               '        ma_same_diff -- 第二第三k线相同允许误差百分比(默认: 3.0)\n' + \
               '        ma_same_days -- 蚂蚁上树之前持续天数(默认: 15)\n' + \
               '        sort_by -- 排序(默认: None, close -- 现价, rise -- 阶段涨幅)'

    async def test(self, code: str, name: str = None) -> Optional[pd.DataFrame]:
        kdata = await self.load_kdata(filter={'code': code,
                                              'trade_date': {'$lte': self.test_end_date}},
                                      limit=self.min_trade_days,
                                      sort=[('trade_date', -1)])

        if kdata is None or kdata.shape[0] < self.min_trade_days:
            return None

        kdata = kdata[::-1]
        for i, v in enumerate(self.ma_numbers):
            kdata[self.ma_numbers_tag[i]] = talib.MA(kdata['close'], timeperiod=v)
        kdata = kdata[::-1]

        fit_days = 0
        ma0_idx, ma2_idx = -1, -1
        for i in range(kdata.shape[0]):
            df = kdata.iloc[i]
            close, rise, ma0, ma2 = df['close'], df['rise'], df[self.ma_numbers_tag[0]], df[self.ma_numbers_tag[2]]
            if np.isnan(ma0) or np.isnan(ma2):
                break
            if close > ma2 and close > ma0 and 0 <= rise <= self.max_climb_up:
                if ma2_idx == -1:
                    i_next = i + 1
                    if i_next >= kdata.shape[0]:
                        break
                    df = kdata.iloc[i_next]
                    close, rise, ma0, ma2 = df['close'], df['rise'], df[self.ma_numbers_tag[0]], df[
                        self.ma_numbers_tag[2]]
                    if ma2 < close and close > ma0 and 0 <= rise <= self.max_climb_up:
                        ma2_idx = i
                    continue
            else:
                if ma2_idx != -1:
                    if close < ma2 and close < ma0 and 0 <= rise <= self.max_climb_up:
                        ma0_idx = i
                        continue
                ma2_idx = -1

            if ma2_idx != -1 and ma0_idx != -1:
                break

        if ma2_idx != -1 and ma0_idx != -1:
            if ma0_idx - ma2_idx >= self.min_climb_days:
                if ma2_idx < self.max_shock_days:
                    return None

                name = await self.code_name(code=code, name=name)
                got_data = dict(code=code, name=name,
                                m0=kdata.iloc[ma0_idx]['trade_date'], m2=kdata.iloc[ma2_idx]['trade_date'],
                                )
                return pd.DataFrame([got_data])

        return None


if __name__ == '__main__':
    from bbq import *

    fund, stock, mysql = default(log_level='error')
    s = AntClimbTree(db=stock)


    async def tt():
        df = await s.run(select_count=2)
        # df = await s.test(code='sh600007')
        await s.plots(df)


    run_until_complete(tt())
