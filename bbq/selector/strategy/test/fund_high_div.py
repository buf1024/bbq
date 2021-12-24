from bbq.selector.strategy.strategy import Strategy
from bbq.data.funddb import FundDB
from datetime import datetime


class FundHighDiv(Strategy):
    def __init__(self, db, *, test_end_date=None, select_count=999999):
        super().__init__(db, test_end_date=test_end_date, select_count=select_count)

        self.year = datetime.now().strftime('%Y')
        self.min_rate = 5
        self.min_count = 2
        self.sort_by = None
        self.interval = 5

    @staticmethod
    def desc():
        return '  名称: 高分红选基金策略\n' + \
               '  说明: 选择高分红高的基金\n' + \
               '  参数: year -- 分红年份(默认: 当前年)\n' + \
               '        min_rate -- 分红比例(默认: 5)\n' + \
               '        min_count -- 分红次数(默认: 2)\n' + \
               '        sort_by -- 结果排序字段(默认: None, 即: rate, rate/count)'

    async def prepare(self, **kwargs):
        """
        初始化接口
        :param kwargs:
        :return: True/False
        """
        await super().prepare(**kwargs)

        if not isinstance(self.db, FundDB):
            self.log.error('数据库参数非基金数据库，只适合选基金')
            return False

        if kwargs is not None and 'year' in kwargs:
            self.year = kwargs['year']
        self.min_rate = kwargs['min_rate'] if kwargs is not None and 'min_rate' in kwargs else 5
        self.min_count = kwargs['min_count'] if kwargs is not None and 'min_count' in kwargs else 2
        self.sort_by = kwargs['sort_by'] if kwargs is not None and 'sort_by' in kwargs else None

        try:
            self.year = str(int(self.year))
            self.min_rate = float(self.min_rate)
            self.min_count = int(self.min_count)
            if self.sort_by is not None:
                if self.sort_by.lower() not in ('rate', 'count'):
                    self.log.error('sort_by不合法')
                    return False
            else:
                self.sort_by = 'rate'

        except ValueError:
            self.log.error('策略参数不合法')
            return False

        return True

    async def destroy(self):
        """
        清理接口
        :return: True/False
        """
        return True

    async def select(self):
        """
        根据策略，选择股票
        :return: [{code, ctx...}, {code, ctx}, ...]/None
        """
        net_df = await self.db.load_fund_net(filter={'dividend': {'$regex': '每份派现金'}},
                                             sort=[('code', 1), ('trade_date', -1)])

        net_df['year'] = net_df['trade_date'].apply(lambda d: d.strftime('%Y'))

        def apply_func(row):
            s = row['dividend'].replace('每份派现金', '')
            s = s.replace('元', '')
            m = float(s)

            if row['net'] > 0:
                row['rate'] = round(m / row['net'] * 100, 2)

            return row

        net_df = net_df.apply(func=apply_func, axis=1)
        group_df = net_df[net_df['year'] == self.year].groupby('code')
        data = group_df.agg({'rate': 'sum', 'year': 'count'})
        data.rename(columns={'year': 'count'}, inplace=True)
        data = data[data['rate'] >= self.min_rate]
        data = data[data['count'] >= self.min_count]
        data = data.sort_values(by=self.sort_by, ascending=False)

        print('done')
        return data

