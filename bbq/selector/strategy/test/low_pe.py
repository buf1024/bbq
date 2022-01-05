from typing import Optional
import pandas as pd
from bbq.selector.strategy.strategy import Strategy


class LowPe(Strategy):
    """
    股价低、位置低、市盈率
    """

    def __init__(self, db, *, test_end_date=None):
        super().__init__(db, test_end_date=test_end_date)
        self.max_pe = 20.0
        self.max_price = 10.9

    @staticmethod
    def desc():
        return '  名称: 右侧选股(基于日线)\n' + \
               '  说明: 选择右侧上涨的股票\n' + \
               '  参数: max_pe -- 最大市盈率(默认: 20)\n' + \
               '        max_price -- 最大股价(默认: 10.0)'

    async def prepare(self, **kwargs):
        """
        初始化接口
        :param kwargs:
        :return: True/False
        """
        await super().prepare(**kwargs)
        try:
            if kwargs is not None and 'max_pe' in kwargs:
                self.max_pe = float(kwargs['max_pe'])
            if kwargs is not None and 'max_price' in kwargs:
                self.max_price = float(kwargs['max_price'])

        except ValueError:
            self.log.error('策略参数不合法')
            return False
        self.is_prepared = True
        return self.is_prepared

    async def test(self, code: str, name: str = None) -> Optional[pd.DataFrame]:
        if self.skip_kcb and code.startswith('sh688'):
            return None

        index_data = await self.db.load_stock_index(
            filter={'code': code, 'pe': {'$lte': self.max_pe}, 'trade_date': self.test_end_date},
            limit=1,
            sort=[('trade_date', -1)])

        if index_data is None or index_data.empty:
            return None

        kdata = await self.load_kdata(
            filter={'code': code, 'close': {'$lte': self.max_price}, 'trade_date': self.test_end_date},
            limit=1,
            sort=[('trade_date', -1)])

        if kdata is None or kdata.empty:
            return None

        name = await self.code_name(code=code, name=name)
        trade_date, pe, pe_ttm = index_data.iloc[0]['trade_date'], index_data.iloc[0]['pe'], index_data.iloc[0][
            'pe_ttm']
        close = kdata.iloc[0]['close']
        got_data = dict(code=code, name=name,
                        trade_date=trade_date, pe=pe, pe_ttm=pe_ttm, close=close)
        return pd.DataFrame([got_data])


if __name__ == '__main__':
    from bbq import *

    fund, stock, mysql = default(log_level='error')
    s = LowPe(db=stock)


    async def tt():
        await s.prepare()
        df = await s.test('sz000558')
        print(df)


    run_until_complete(tt())
