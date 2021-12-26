from typing import Optional
import pandas as pd
from bbq.selector.strategy.strategy import Strategy


class RightSide(Strategy):
    """
    看第1天，第2天是否为涨，形态逐渐放大，下落不多，可追
    """

    def __init__(self, db, *, test_end_date=None, select_count=999999):
        super().__init__(db, test_end_date=test_end_date, select_count=select_count)
        self.min_trade_days = 60
        self.min_con_days = 3
        self.max_con_down = -2.0
        self.max_con_up = 20.0
        self.judge_days = 8
        self.judge_days_up = 15.0
        self.sort_by = None

    @staticmethod
    def desc():
        return '  名称: 右侧选股(基于日线)\n' + \
               '  说明: 选择右侧上涨的股票\n' + \
               '  参数: min_trade_days -- 最小上市天数(默认: 60)\n' + \
               '        min_con_days -- 最近最小连续上涨天数(默认: 3)\n' + \
               '        max_con_down -- 视为上涨最大下跌百分比(默认: -2.0)\n' + \
               '        max_con_up -- 视为上涨最大上涨百分比(默认: 20.0)\n' + \
               '        judge_days -- judge_days内judge_days_up天数(默认: 8)\n' + \
               '        judge_days_up -- 最近judge_days内上涨百分比(默认: 15.0)\n' + \
               '        sort_by -- 排序(默认: None, close -- 现价, rise -- 阶段涨幅)'

    async def prepare(self, **kwargs):
        """
        初始化接口
        :param kwargs:
        :return: True/False
        """
        await super(RightSide, self).prepare(**kwargs)
        try:
            if kwargs is not None and 'min_trade_days' in kwargs:
                self.min_trade_days = int(kwargs['min_trade_days'])
            if kwargs is not None and 'min_con_days' in kwargs:
                self.min_con_days = int(kwargs['min_con_days'])
            if kwargs is not None and 'max_con_down' in kwargs:
                self.max_con_down = float(kwargs['max_con_down'])
            if kwargs is not None and 'max_con_up' in kwargs:
                self.max_con_up = float(kwargs['max_con_up'])
            if kwargs is not None and 'judge_days' in kwargs:
                self.judge_days = int(kwargs['judge_days'])
                if self.judge_days <= 0 or self.judge_days > self.min_trade_days:
                    self.log.error('策略参数judge_days不合法: {}~{}'.format(0, self.min_trade_days))
                    return False
            if kwargs is not None and 'judge_days_up' in kwargs:
                self.judge_days_up = float(kwargs['judge_days_up'])

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
            if self.max_con_down <= rise <= self.max_con_up:
                fit_days = fit_days + 1
                continue
            break

        if fit_days < self.min_con_days:
            return None

        jd_close, now_close = kdata.iloc[self.judge_days - 1]['close'], kdata.iloc[0]['close']
        rise = round((now_close - jd_close) * 100 / jd_close, 2)
        if rise >= self.judge_days_up:
            name = await self.code_name(code=code, name=name)
            got_data = dict(code=code, name=name,
                            close=now_close, judge_close=jd_close, cont_day=fit_days, rise=rise,
                            condition='cont({}%~{}%): {}\n{}days up: {}%'.format(
                                self.max_con_down, self.max_con_up, self.min_con_days,
                                self.judge_days, self.judge_days_up))
            return pd.DataFrame([got_data])

        return None


if __name__ == '__main__':
    from bbq import *

    fund, stock, mysql = default(log_level='error')
    s = RightSide(db=stock)


    async def tt():
        df = await s.run(min_con_days=2, max_con_up=5, sort_by='rise', judge_days_up=8)
        await s.plots(df)


    run_until_complete(tt())
