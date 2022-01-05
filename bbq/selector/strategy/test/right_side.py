from typing import Optional
import pandas as pd
from bbq.selector.strategy.strategy import Strategy


class RightSide(Strategy):
    """
    看上方有没有抛压，上涨幅度。
    貌似上涨30%的都可以追，上涨大于40%有一定下跌的概率
    示意形态:
       |
      |
     |
    |
    """

    def __init__(self, db, *, test_end_date=None):
        super().__init__(db, test_end_date=test_end_date)
        self.min_rise_days = 3
        self.max_down_in_rise = -2.0
        self.max_up_in_rise = 20.0
        self.max_leg_ratio = 33.3
        self.recent_ndays = 8
        self.recent_days_up = 15.0

    @staticmethod
    def desc():
        return '  名称: 右侧选股(基于日线)\n' + \
               '  说明: 选择右侧上涨的股票\n' + \
               '  参数: min_rise_days -- 最近最小连续上涨天数(默认: 3)\n' + \
               '        max_down_in_rise -- 最大下跌百分比(默认: -2.0)\n' + \
               '        max_up_in_rise -- 最大上涨百分比(默认: 20.0)\n' + \
               '        max_leg_ratio -- 上涨最大腿长(默认: 33.3)\n' + \
               '        recent_days -- 最近累计计算涨幅天数(默认: 8)\n' + \
               '        recent_days_up -- 最近judge_days内上涨百分比(默认: 15.0)'

    async def prepare(self, **kwargs):
        """
        初始化接口
        :param kwargs:
        :return: True/False
        """
        await super(RightSide, self).prepare(**kwargs)
        try:
            if kwargs is not None and 'min_rise_days' in kwargs:
                self.min_rise_days = int(kwargs['min_rise_days'])
            if kwargs is not None and 'max_down_in_rise' in kwargs:
                self.max_down_in_rise = float(kwargs['max_down_in_rise'])
            if kwargs is not None and 'max_up_in_rise' in kwargs:
                self.max_up_in_rise = float(kwargs['max_up_in_rise'])
            if kwargs is not None and 'max_leg_ratio' in kwargs:
                self.max_leg_ratio = float(kwargs['max_leg_ratio'])
            if kwargs is not None and 'recent_ndays' in kwargs:
                self.recent_ndays = int(kwargs['recent_ndays'])
                if self.recent_ndays <= 0 or self.recent_ndays > self.min_trade_days:
                    self.log.error('策略参数recent_ndays不合法: {}~{}'.format(0, self.min_trade_days))
                    return False
            if kwargs is not None and 'recent_days_up' in kwargs:
                self.recent_days_up = float(kwargs['recent_days_up'])

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
        for df in kdata.to_dict('records'):
            rise = df['rise']
            if self.max_down_in_rise <= rise <= self.max_up_in_rise and \
                    self.is_short_leg(df, self.max_leg_ratio, 'top'):
                fit_days = fit_days + 1
                continue
            break

        if fit_days < self.min_rise_days:
            return None

        re_close, now_close = kdata.iloc[self.recent_ndays - 1]['close'], kdata.iloc[0]['close']
        recent_rise = round((now_close - re_close) * 100 / re_close, 2)
        if recent_rise >= self.recent_days_up:
            name = await self.code_name(code=code, name=name)
            got_data = dict(code=code, name=name,
                            nday_close=re_close, close=now_close, nday_rise=recent_rise,
                            rise_start=kdata.iloc[fit_days]['trade_date'],
                            rise_days=fit_days)
            return pd.DataFrame([got_data])

        return None


if __name__ == '__main__':
    from bbq import *

    fund, stock, mysql = default(log_level='error')
    s = RightSide(db=stock)


    async def tt():
        await s.prepare(min_rise_days=2, max_down_in_rise=-1,
                        max_up_in_rise=20, max_leg_ratio=40,
                        recent_days=8, recent_days_up=10, sort_by='rise', )
        df = await s.test('sz000558')
        print(df)


    run_until_complete(tt())
