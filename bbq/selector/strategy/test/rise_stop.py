from typing import Optional
import pandas as pd
from bbq.selector.strategy.strategy import Strategy


class RiseStop(Strategy):
    """
    涨停后，第二天高位开盘收涨，第三天可能有好的表现。
    """

    def __init__(self, db, *, test_end_date=None):
        super().__init__(db, test_end_date=test_end_date)
        self.min_stop_rise = 9
        self.min_cy_stop_rise = 19
        self.min_high_rise = 8
        self.min_close_rise = 0.5

    @staticmethod
    def desc():
        return '  名称: 涨停开板选股(基于日线)\n' + \
               '  说明: 选择涨停开板选股股票\n' + \
               '  参数: min_high_rise -- 涨停次日至少最大上涨百分比(默认: 9)\n' + \
               '        min_close_rise -- 收盘上涨百分比(默认: 0.5)\n' + \
               '        min_stop_rise -- 非创业板视为涨停百分比(默认: 9)\n' + \
               '        min_cy_stop_rise -- 创业板收盘上涨百分比(默认: 19)'

    async def prepare(self, **kwargs):
        """
        初始化接口
        :param kwargs:
        :return: True/False
        """
        await super().prepare(**kwargs)
        try:
            if kwargs is not None and 'min_high_rise' in kwargs:
                self.min_high_rise = float(kwargs['min_high_rise'])
            if kwargs is not None and 'min_close_rise' in kwargs:
                self.min_close_rise = float(kwargs['min_close_rise'])
            if kwargs is not None and 'min_stop_rise' in kwargs:
                self.min_stop_rise = float(kwargs['min_stop_rise'])
            if kwargs is not None and 'min_cy_stop_rise' in kwargs:
                self.min_cy_stop_rise = float(kwargs['min_cy_stop_rise'])
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

        stop_days, high_rise = 0, 0
        for index, df in enumerate(kdata.to_dict('records')):
            rise = df['rise']
            if index == 0:
                last_close = df['close'] - df['diff']
                high_rise = ((df['high'] - last_close) * 100) / last_close
                if high_rise < self.min_high_rise:
                    return None
                if rise < self.min_close_rise:
                    return None
                if rise >= self.min_high_rise:
                    return None
            else:
                min_stop_rise = self.min_cy_stop_rise if code.startswith('sh688') or code.startswith(
                    'sz30') else self.min_stop_rise
                if rise >= min_stop_rise:
                    stop_days = stop_days + 1
                    continue
                break

        if stop_days <= 0:
            return None

        begin_close = kdata.iloc[stop_days]['close'] - kdata.iloc[stop_days]['diff']
        acct_rise = round((kdata.iloc[0]['close'] - begin_close)*100 / begin_close, 2)
        name = await self.code_name(code=code, name=name)
        got_data = dict(code=code, name=name,
                        rise_stop_start=kdata.iloc[stop_days]['trade_date'],
                        rise_stop_days=stop_days,
                        last_high_rise=high_rise,
                        last_rise=kdata.iloc[0]['rise'],
                        acct_rise=acct_rise)
        return pd.DataFrame([got_data])


if __name__ == '__main__':
    from bbq import *

    fund, stock, mysql = default(log_level='error')
    s = RiseStop(db=stock, test_end_date='2022-02-09')


    async def tt():
        await s.prepare(min_stop_rise=9,
                        min_cy_stop_rise=19,
                        min_high_rise=8,
                        min_close_rise=0.5)
        df = await s.test('sz301027')
        print(df)


    run_until_complete(tt())
