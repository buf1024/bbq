from datetime import datetime

from bbq.selector.strategy.strategy import Strategy


class AntClimbTree(Strategy):
    def __init__(self, db):
        super().__init__(db)
        self.now = datetime.now()

        self.end_time = datetime(year=self.now.year, month=self.now.month, day=self.now.day)
        self.test_days = 30  # 计算天数
        self.left_days = 5  # 左侧计算天数
        self.left_up = 0.1  # 左侧累计最小涨幅
        self.right_min_days = 3  # 右侧最小天数
        self.right_max_days = 10  # 右侧最大天数
        self.right_max_up = 0.02  # 右侧最大上涨
        self.right_max_down = 0.1  # 右侧最大下跌

    async def prepare(self, **kwargs):
        self.end_time = kwargs['end_time'] if kwargs is not None and 'end_time' in kwargs else self.end_time
        self.test_days = kwargs['test_days'] if kwargs is not None and 'test_days' in kwargs else self.test_days
        self.left_up = kwargs['left_up'] if kwargs is not None and 'left_up' in kwargs else self.left_up
        self.right_min_days = kwargs['right_min_days'] if kwargs is not None and 'right_min_days' in kwargs \
            else self.right_min_days
        self.right_max_days = kwargs['right_max_days'] if kwargs is not None and 'right_max_days' in kwargs \
            else self.right_max_days
        self.right_max_up = kwargs['right_max_up'] if kwargs is not None and 'right_max_up' in kwargs \
            else self.right_min_days
        self.right_max_down = kwargs['right_max_down'] if kwargs is not None and 'right_max_down' in kwargs \
            else self.right_max_down

        try:
            self.end_time = self.end_time if not isinstance(self.end_time, str) else datetime.strftime('%Y-%m-%d')
            self.test_days = int(self.test_days)
            self.left_up = float(self.left_up)
            self.right_min_days = int(self.right_min_days)
            self.right_max_days = int(self.right_max_days)
            self.right_max_up = float(self.right_max_up)
            self.right_max_down = float(self.right_max_down)
        except ValueError:
            self.log.error('策略参数不为整数')
            return False

        return True

    def desc(self):
        return '  名称: 蚂蚁上树形态(变形)策略\n' + \
               '  说明: 前n日，找出成交量最大而且是上涨的一天，右侧m天上涨或下跌平缓(成交量减少)，左侧x天持续上涨y%\n' + \
               '  参数: end_time -- 计算开始时间(默认当天)\n' \
               '        test_days -- 计算往前推的交易日(默认30)\n' \
               '        left_up -- 左侧默认上涨幅度(默认0.1)' \
               '        right_min_days -- 右侧计算最小天数(默认3)' \
               '        right_max_days -- 右侧计算最大天数(默认10)' \
               '        right_max_up -- 右侧计算最大上涨(默认0.02)' \
               '        right_max_down -- 右侧计算最大下跌(默认0.01)'

    async def select(self):
        """
        根据策略，选择股票
        :return: :return: [{code, ctx...}, {code, ctx}, ...]/None
        """
        codes = await self.db.load_stock_info(projection=['code'])
        if codes is None or codes.empty:
            self.log.error('数据库无股票信息')
            return None

        select_codes = []
        info_dict = {}
        for code in codes['code'].values:
            daily = await self.db.load_stock_daily(filter={'code': code, 'trade_date': {'$lte': self.end_time}},
                                                   sort=[('trade_date', -1)])
            if daily is None or daily.empty:
                continue

            if daily.shape[0] < self.test_days:
                continue

            # todo
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
                      '比例({:.2f}%), 涨幅: {:.2f}%'.format(code, first_up,
                                                        daily.iloc[idxmax]['close'],
                                                        daily.iloc[idxmax]['trade_date'].strftime('%Y-%m-%d'),
                                                        daily_new.iloc[idxmin]['close'],
                                                        daily_new.iloc[idxmin]['trade_date'].strftime('%Y-%m-%d'),
                                                        total, up, down, chg * 100, total_chg * 100)
                # self.log.debug(msg)
                if chg > self.ratio_up and total <= self.low_max_date:
                    info_dict[code] = dict(chg=chg, msg=msg)
                    select_codes.append(code)
        select_codes = sorted(select_codes, key=lambda c: info_dict[c]['chg'], reverse=True)

        for code in select_codes:
            self.log.info(info_dict[code]['msg'])

        return select_codes
