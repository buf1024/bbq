from bbq.fetch import fetch_stock_new_quote
from bbq.selector.strategy.strategy import Strategy
import pandas as pd


class StockNewFresh(Strategy):
    def __init__(self, db, *, test_end_date=None, select_count=999999):
        super().__init__(db, test_end_date=test_end_date, select_count=select_count)
        self.trade_date = 60
        self.ratio_up = 0.5
        self.low_max_date = 10
        self.first_up = 2

    async def prepare(self, **kwargs):
        await super().prepare(**kwargs)

        self.trade_date = kwargs['trade_date'] if kwargs is not None and 'trade_date' in kwargs else 60
        self.ratio_up = kwargs['ratio_up'] if kwargs is not None and 'ratio_up' in kwargs else 0.5
        self.low_max_date = kwargs['low_max_date'] if kwargs is not None and 'low_max_date' in kwargs else 10
        self.first_up = kwargs['first_up'] if kwargs is not None and 'first_up' in kwargs else 2

        try:
            self.trade_date = int(self.trade_date)
            self.ratio_up = float(self.ratio_up)
            self.low_max_date = int(self.low_max_date)
            self.first_up = int(self.first_up)
            if self.trade_date <= 0 or self.ratio_up <= 0 \
                    or self.low_max_date <= 0 or self.first_up <= 0:
                self.log.error('策略参数不为正整数')
                return False
        except ValueError:
            self.log.error('策略参数不为整数')
            return False

        return True

    @staticmethod
    def desc():
        return '  名称: 次新股板块策略\n' + \
               '  说明: 第一次新高后，根据新低反弹情况选股\n' + \
               '  参数: trade_date -- 上市交易最多天数(默认60)\n' \
               '        low_max_date -- 首低后最多交易天数(默认10)\n' \
               '        first_up -- 上市后默认上涨天数(默认2)' \
               '        ratio_up -- 首低后涨跌比(默认0.5)'

    async def select(self):
        """
        根据策略，选择股票
        :return: [{code, ctx...}, {code, ctx}, ...]/None
        """
        quotes = fetch_stock_new_quote()
        if quotes is None:
            self.log.error('获取次新股行情失败')
            return None
        codes = [code for code in quotes['code'].tolist() if
                 await self.db.stock_daily.count_documents({'code': code}) <= self.trade_date]

        select_codes = []
        for code in codes:
            daily = await self.db.load_stock_daily(filter={'code': code,
                                                           'trade_date': {'$lte': self.test_end_date}},
                                                   sort=[('trade_date', -1)])
            if daily is None:
                self.log.error('没有k线数据')
                continue

            # 计算第一次新高, 2020-10-10 - 2020-10-11 < 0 + > 0 -
            diff = daily['close'].diff()[1:]
            if len(diff) == 0:
                continue
            idxmax = daily.shape[0] - 1
            first_up = 0
            diff = diff.reset_index(drop=True)
            for close_diff in reversed(diff):
                # 开始跌
                if close_diff > 0:
                    break
                idxmax = idxmax - 1
                first_up = first_up + 1

            if idxmax > 0 and first_up > self.first_up:
                daily_new = daily[:idxmax]
                # 计算新高后第一次新低, 2020-10-10 - 2020-10-11  < 0 + > 0 -
                diff = daily_new['close'].diff()[1:]
                if len(diff) == 0:
                    continue
                idxmin = daily_new.shape[0] - 1
                diff = diff.reset_index(drop=True)
                for close_diff in reversed(diff):
                    # 开始涨
                    if close_diff < 0:
                        break
                    idxmin = idxmin - 1
                if idxmin < 0:
                    continue
                # idxmin = daily_new['close'].idxmin()
                diff = daily_new[:idxmin + 1]['close'].diff()[1:]
                diff_up = [x for x in diff if x < 0]
                total, up, down = len(diff), len(diff_up), len(diff) - len(diff_up)
                chg = up * 1.0 / total if total > 0 else 0.0

                total_chg = (daily.iloc[0]['close'] - daily_new.iloc[idxmin]['close']) / daily_new.iloc[idxmin]['close']
                msg = '代码 {}, 上市上涨天数({}), 首高({}, {}), 首低({}, {}), 首低后{}交易日: 上升({}), 下降({}), ' \
                      '上升比例({:.2f}%), 涨幅: {:.2f}%'.format(code, first_up,
                                                          daily.iloc[idxmax]['close'],
                                                          daily.iloc[idxmax]['trade_date'].strftime('%Y-%m-%d'),
                                                          daily_new.iloc[idxmin]['close'],
                                                          daily_new.iloc[idxmin]['trade_date'].strftime('%Y-%m-%d'),
                                                          total, up, down, chg * 100, total_chg * 100)
                # self.log.debug(msg)
                if chg > self.ratio_up and total <= self.low_max_date:
                    select_codes.append({'code': code,
                                         'name': quotes[quotes['code'] == code].iloc[0]['name'],
                                         '上市上涨天数': first_up,
                                         '首高': daily.iloc[idxmax]['close'],
                                         '首高日期': daily.iloc[idxmax]['trade_date'],
                                         '首低': daily_new.iloc[idxmin]['close'],
                                         '首低日期': daily_new.iloc[idxmin]['trade_date'],
                                         '首低后交易日': total,
                                         '上升': up,
                                         '下降': down,
                                         '上升比例': chg,
                                         '涨幅': total_chg,
                                         'msg': msg})
        df = None
        if len(select_codes) > 0:
            select_codes = sorted(select_codes, key=lambda c: c['上升比例'], reverse=True)
            df = pd.DataFrame(select_codes)

        return df



