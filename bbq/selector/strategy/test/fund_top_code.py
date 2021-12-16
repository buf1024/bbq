from bbq.selector.strategy.strategy import Strategy
from bbq.data.funddb import FundDB
from bbq.analyse.tools import linear_fitting
import pandas as pd
from tqdm import tqdm
from datetime import timedelta, datetime


class FundTopCode(Strategy):
    def __init__(self, db):
        super().__init__(db)
        self.days = 30
        self.min_days = 10
        self.coef = None
        self.score = None
        self.sort_by = None

    def desc(self):
        return '  名称: 龙头选基金策略(基于净值)\n' + \
               '  说明: 选择上涨趋势的选股\n' + \
               '  参数: days -- 最近交易天数(默认: 30)\n' + \
               '        min_days -- 最小净值天数(默认: 10)\n' + \
               '        coef -- 线性拟合系数(默认: None)\n' + \
               '        score -- 线性拟合度(默认: None)\n' + \
               '        sort_by -- 结果排序字段(默认: None, 即: score)'

    async def prepare(self, **kwargs):
        """
        初始化接口
        :param kwargs:
        :return: True/False
        """
        if not isinstance(self.db, FundDB):
            self.log.error('数据库不合法，非FundDB')
            return False

        self.days = kwargs['days'] if kwargs is not None and 'days' in kwargs else 30
        self.min_days = kwargs['min_days'] if kwargs is not None and 'min_days' in kwargs else 10
        self.coef = kwargs['coef'] if kwargs is not None and 'coef' in kwargs else None
        self.score = kwargs['score'] if kwargs is not None and 'score' in kwargs else None
        self.sort_by = kwargs['sort_by'] if kwargs is not None and 'sort_by' in kwargs else None

        try:
            self.days = int(self.days)
            self.min_days = int(self.min_days)
            if self.coef is not None:
                self.coef = float(self.coef)
            if self.score is not None:
                self.score = float(self.score)
            if self.sort_by is not None:
                if self.sort_by.lower() not in ('coef', 'score'):
                    self.log.error('sort_by不合法')
                    return False
            else:
                self.sort_by = 'score'

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
        根据策略，选择基金
        :return: code, name, coef(系数), score(分数), rise(累计涨幅)
        """

        data = await self.db.load_fund_net(limit=1, sort=[('trade_date', -1)])
        if data is None or data.shape[0] <= 0:
            self.log.error('数据为空')
            return None

        data['net_acc'] = data['net_acc'].apply(lambda x: float(x))

        day_mx = data.iloc[0]['trade_date']
        day_mx = datetime(year=day_mx.year, month=day_mx.month, day=day_mx.day)
        day_cond = day_mx + timedelta(days=-self.days)
        data = await self.db.load_fund_net(filter={'trade_date': {'$gte': day_cond}}, sort=[('trade_date', -1)])

        data['net_acc'] = data['net_acc'].apply(lambda x: float(x))

        group_data = data.groupby('code')
        select = []
        proc_bar = tqdm(group_data.groups.items())
        for code, indexes in proc_bar:
            proc_bar.set_description('处理 {}'.format(code))
            cal_data = data.iloc[indexes]
            cal_data = cal_data[::-1]

            if cal_data.shape[0] < self.min_days:
                continue

            name_df = await self.db.load_fund_info(filter={'code': code})
            name = name_df.iloc[0]['name']

            rise = round(
                (cal_data.iloc[-1]['net_acc'] - cal_data.iloc[0]['net_acc']) * 100 / cal_data.iloc[0][
                    'net_acc'], 2)
            a, b, score, x_index, y_index = linear_fitting(cal_data, field='net_acc')
            if a is None or b is None or x_index is None or y_index is None:
                continue
            a, b, score = round(a, 4), round(b, 4), round(score, 4)
            if self.coef is not None and self.score is not None:
                if a > self.coef and score > self.score:
                    got_data = dict(code=code, name=name, coef=a, score=score, rise=rise / 100)
                    self.log.info('got data: {}'.format(got_data))
                    select.append(got_data)
            else:
                got_data = dict(code=code, name=name, coef=a, score=score, rise=rise / 100)
                select.append(got_data)

        proc_bar.close()

        df = None
        if len(select) > 0:
            select = sorted(select, key=lambda v: v[self.sort_by], reverse=True)
            df = pd.DataFrame(select)

        return df


