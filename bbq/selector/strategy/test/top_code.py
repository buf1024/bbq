from bbq.selector.strategy.strategy import Strategy
from bbq.data.stockdb import StockDB
from bbq.analyse.tools import linear_fitting


class TopCode(Strategy):
    def __init__(self, db):
        super().__init__(db)
        self.days = 30
        self.min_days = 10
        self.coef = None
        self.score = None
        self.sort_by = None

    def desc(self):
        return '  名称: 龙头选股策略\n' + \
               '  说明: 选择上涨趋势的选股\n' + \
               '  参数: days -- 最近交易天数(默认: 30)\n' + \
               '        min_days -- 最小上市天数(默认: 10)\n' + \
               '        coef -- 线性拟合系数(默认: None)\n' + \
               '        score -- 线性拟合度(默认: None)\n' + \
               '        sort_by -- 结果排序字段(默认: None, 即: score)'

    async def init(self, **kwargs):
        """
        初始化接口
        :param kwargs:
        :return: True/False
        """
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
        根据策略，选择股票
        :return: [code, code, ...]/None
        """
        codes = None
        if isinstance(self.db, StockDB):
            codes = await self.db.load_stock_info(projection=['code', 'name'])
        else:
            codes = await self.db.load_fund_info(filter={'type': {'$regex': '场内'}}, projection=['code', 'name'])

        select = []
        for item in codes.to_dict('records'):
            kdata = None
            if isinstance(self.db, StockDB):
                kdata = await self.db.load_stock_daily(filter={'code': item['code']}, limit=self.days,
                                                       sort=[('trade_date', -1)])
            else:
                kdata = await self.db.load_fund_daily(filter={'code': item['code']}, limit=self.days,
                                                      sort=[('trade_date', -1)])
            if kdata is None or kdata.shape[0] < self.min_days:
                continue
            kdata = kdata[::-1]

            rise = round((kdata.iloc[-1]['close'] - kdata.iloc[0]['close']) * 100 / kdata.iloc[0]['close'], 2)
            a, b, score, x_index, y_index = linear_fitting(kdata)
            a, b, score = round(a, 4), round(b, 4), round(score, 4)
            if self.coef is not None and self.score is not None:
                if a > self.coef and score > self.score:
                    print('got you: a={}, score={}, {}({})'.format(a, score, item['name'], item['code']))
                    select.append(dict(coef=a, score=score, rise=rise, code=item['code'], name=item['name']))
            else:
                select.append(dict(coef=a, score=score, rise=rise, code=item['code'], name=item['name']))
        if len(select) > 0:
            select = sorted(select, key=lambda v: v[self.sort_by], reverse=True)

        print('done')
        return select
