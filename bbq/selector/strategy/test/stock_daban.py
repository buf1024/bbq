from typing import Optional
import pandas as pd
from bbq.selector.strategy.strategy import Strategy
import math


class StockDaban(Strategy):
    """
    打板股票。
    """

    def __init__(self, db, *, test_end_date=None):
        super().__init__(db, test_end_date=test_end_date)
        self.max_open_stop = 3
        self.max_low_stop = 4

    @staticmethod
    def desc():
        return '  名称: 打板选股(基于日线)\n' + \
               '  说明: 选择强势涨停的股票\n' + \
               '  参数: max_open_stop -- 开盘价距离涨停价最大百分比(默认: 3)\n' + \
               '        max_low_stop -- 最低价价距离涨停价最大百分比(默认: 4)'

    async def prepare(self, **kwargs):
        """
        初始化接口
        :param kwargs:
        :return: True/False
        """
        await super().prepare(**kwargs)
        try:
            if kwargs is not None and 'max_open_stop' in kwargs:
                self.max_open_stop = int(kwargs['max_open_stop'])
            if kwargs is not None and 'max_low_stop' in kwargs:
                self.max_low_stop = float(kwargs['max_low_stop'])

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

        pre_close, now_close = kdata.iloc[1]['close'], kdata.iloc[0]['close']
        now_open, now_low = kdata.iloc[0]['open'], kdata.iloc[0]['low']

        ratio = 1.2 if code.startswith('sz300') or code.startswith('sh688') else 1.1
        expect = math.floor(pre_close*ratio * 100.0) / 100.0
        if now_close < expect:
            return None

        expect = math.floor(pre_close*(ratio - (self.max_open_stop / 100.0))*100.0) / 100.0
        if now_open < expect:
            return None

        expect = math.floor(pre_close*(ratio - (self.max_low_stop / 100.0))*100.0) / 100.0
        if now_low < expect:
            return None

        name = await self.code_name(code=code, name=name)
        got_data = dict(code=code, name=name, close=now_close, rise=kdata.iloc[0]['rise'])
        return pd.DataFrame([got_data])


if __name__ == '__main__':
    from bbq import *

    fund, stock, mysql = default(log_level='error')
    s = StockDaban(db=stock, test_end_date='20220106')


    async def tt():
        await s.prepare()
        df = await s.test('sz002432')
        print(df)


    run_until_complete(tt())
