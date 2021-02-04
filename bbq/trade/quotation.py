from abc import ABC
from collections import OrderedDict
from datetime import datetime, timedelta, date
from typing import Dict, Optional, Tuple
import bbq.fetch as fetch
import bbq.log as log
from bbq.data.mongodb import MongoDB
import traceback
from bbq.data.stockdb import StockDB
from bbq.data.funddb import FundDB
from bbq.trade.enum import event
import pandas as pd

"""
行情下发顺序：
-> evt_start(backtest) -> evt_morning_start -> evt_quotation -> evt_morning_end 
                       -> evt_noon_start -> evt_quotation -> evt_noon_end 
-> evt_end(backtest)

payload 字典:
- evt_start
{
    "frequency": '60min', 
    'start': datetime.datetime(2021, 2, 2, 14, 17, 56, 830798),
    'end': datetime.datetime(2021, 2, 2, 14, 17, 56, 830798)
}

- evt_morning_start
{
    'frequency': '60min',
    'trade_date': datetime.datetime(2020, 12, 1, 0, 0),
    'day_time': Timestamp('2020-12-01 10:30:00')
}

- evt_quotation
{
    'frequency': '60min', 
    'trade_date': datetime.datetime(2020, 12, 1, 0, 0), 
    'day_time': datetime.datetime(2021, 2, 2, 14, 17, 56, 856644), 
    'list': {
        'sh000001': {
            'day': '2020-12-01 10:30:00', 
            'open': 3388.987, 
            'high': 3413.278, 
            'low': 3413.278, 
            'close': 3413.278, 
            'volume': 10867266500, 
            'code': 'sh000001', 
            'day_time': Timestamp('2020-12-01 10:30:00'), 
            'name': '上证综指', 
            'day_high': 3413.278, 
            'day_min': 3413.278, 
            'day_open': 3451.94
        }
        ... // 其他code行情
    }
}

2021-02-02 14:17:56,858 - Dummy - INFO - dummy risk on_quot: 
"""


class Quotation(ABC):
    def __init__(self, db: MongoDB):
        self.log = log.get_logger(self.__class__.__name__)
        self.db_data = db
        self.opt = None

        self.frequency = 0
        self.start_date = None
        self.end_date = None
        self.codes = []

        self.trade_date = None
        self.quot_date = {}

        self.code_info = {}

        self.index = []

        self.day_frequency = 0

    def is_index(self, code: str) -> bool:
        return code in self.index

    async def init(self, opt) -> bool:
        self.opt = opt
        try:
            frequency, codes = self.opt['frequency'].lower(), self.opt['codes']
            if 'min' not in frequency and 'm' not in frequency and \
                    'sec' not in frequency and 's' not in frequency and \
                    'day' not in frequency and 'd' not in frequency:
                self.log.error('frequency 格式不正确')
                return False
            if frequency.endswith('min') or frequency.endswith('m'):
                value = int(frequency.split('m')[0])
                if value <= 0:
                    self.log.error('frequency 格式不正确')
                    return False

                self.frequency = value * 60

            if frequency.endswith('sec') or frequency.endswith('s'):
                value = int(frequency.split('s')[0])
                if value <= 0:
                    self.log.error('frequency 格式不正确')
                    return False

                self.frequency = value

            if frequency.endswith('day') or frequency.endswith('d'):
                value = int(frequency.split('d')[0])
                if value <= 0:
                    self.log.error('frequency 格式不正确')
                    return False

                self.frequency = value * 60 * 60 * 24
                self.day_frequency = value

            if 'start_date' in self.opt and self.opt['start_date'] is not None:
                self.start_date = self.opt['start_date']
                if isinstance(self.start_date, date):
                    self.start_date = datetime(year=self.start_date.year,
                                               month=self.start_date.month,
                                               day=self.start_date.day)

            if 'end_date' in self.opt and self.opt['end_date'] is not None:
                self.end_date = self.opt['end_date']
                if isinstance(self.end_date, date):
                    self.end_date = datetime(year=self.end_date.year,
                                             month=self.end_date.month,
                                             day=self.end_date.day)

            if not await self.add_code(codes):
                self.log.error('获取股票信息失败')
                return False

            return True

        except Exception as e:
            self.log.error('quot 初始化失败, ex={}, callstack={}'.format(e, traceback.format_exc()))
            return False

    async def get_quot(self) -> Optional[Tuple[Optional[str], Optional[Dict]]]:
        return None, None

    async def add_code(self, codes) -> bool:
        new_codes = []
        for code in codes:
            if code not in self.codes:
                new_codes.append(code)
        if len(new_codes) == 0:
            return False
        self.codes = self.codes + new_codes
        df = None
        if isinstance(self.db_data, StockDB):
            df1 = await self.db_data.load_index_info(filter={'code': {'$in': self.codes}}, projection=['code', 'name'])
            if df1 is not None or not df1.empty:
                df = df1
                for data in df.to_dict('records'):
                    self.index.append(data['code'])

            df2 = await self.db_data.load_stock_info(filter={'code': {'$in': self.codes}}, projection=['code', 'name'])
            if df2 is not None or not df2.empty:
                if df is None:
                    df = df2
                else:
                    df = pd.concat((df, df2))
        elif isinstance(self.db_data, FundDB):
            df = await self.db_data.load_fund_info(filter={'code': {'$in': self.codes}}, projection=['code', 'name'])
        if df is None:
            self.log.error('db stock/fund info failed')
            return False

        self.code_info.clear()
        for data in df.to_dict('records'):
            self.code_info[data['code']] = data['name']
        return True

    def is_trading(self) -> bool:
        if self.trade_date is None:
            return False

        status_dict = self.quot_date[self.trade_date]

        if (status_dict[event.evt_morning_start] and not status_dict[event.evt_morning_end]) or \
                (status_dict[event.evt_noon_start] and not status_dict[event.evt_noon_end]):
            return True

        return False

    def get_status_dict(self, now):
        date_now = datetime(year=now.year, month=now.month, day=now.day)
        if len(self.quot_date) == 0 or date_now not in self.quot_date:
            self.trade_date = None
            self.quot_date.clear()
            is_open = fetch.is_trade_date(date_now)

            self.quot_date[date_now] = dict(is_open=is_open,
                                            evt_morning_start=False,
                                            evt_morning_end=False,
                                            evt_noon_start=False,
                                            evt_noon_end=False)
        self.trade_date = date_now
        return self.quot_date[date_now]

    async def get_base_event(self, now) -> Optional[Tuple[Optional[str], Optional[Dict]]]:
        status_dict = self.get_status_dict(now)

        if not status_dict['is_open']:
            return None, None

        morning_start_date = datetime(year=now.year, month=now.month, day=now.day, hour=9, minute=30, second=0)
        morning_end_date = datetime(year=now.year, month=now.month, day=now.day, hour=11, minute=30, second=0)

        noon_start_date = datetime(year=now.year, month=now.month, day=now.day, hour=13, minute=0, second=0)
        noon_end_date = datetime(year=now.year, month=now.month, day=now.day, hour=15, minute=0, second=0)

        def _base_event_emit(evt):
            if not status_dict[evt]:
                status_dict[evt] = True
                return evt, dict(frequency=self.opt['frequency'],
                                 trade_date=self.trade_date,
                                 day_time=now)
            return None, None

        if morning_start_date <= now < morning_end_date:
            # if not status_dict[event.evt_morning_start]:
            #     status_dict[event.evt_morning_start] = True
            #     return event.evt_morning_start, dict(frequency=self.opt['frequency'],
            #                                          trade_date=self.trade_date,
            #                                          day_time=now)
            evt, playload = _base_event_emit(event.evt_morning_start)
            if evt is not None:
                return evt, playload
        elif morning_end_date <= now < noon_start_date:
            evt, playload = _base_event_emit(event.evt_morning_start)
            if evt is not None:
                return evt, playload
            evt, playload = _base_event_emit(event.evt_morning_end)
            if evt is not None:
                return evt, playload
        elif noon_start_date <= now < noon_end_date:
            evt, playload = _base_event_emit(event.evt_morning_start)
            if evt is not None:
                return evt, playload
            evt, playload = _base_event_emit(event.evt_morning_end)
            if evt is not None:
                return evt, playload
            evt, playload = _base_event_emit(event.evt_noon_start)
            if evt is not None:
                return evt, playload
        elif now >= noon_end_date:
            evt, playload = _base_event_emit(event.evt_morning_start)
            if evt is not None:
                return evt, playload
            evt, playload = _base_event_emit(event.evt_morning_end)
            if evt is not None:
                return evt, playload
            evt, playload = _base_event_emit(event.evt_noon_start)
            if evt is not None:
                return evt, playload
            evt, playload = _base_event_emit(event.evt_noon_end)
            if evt is not None:
                return evt, playload

        return None, None


class BacktestQuotation(Quotation):
    def __init__(self, db: MongoDB):
        super().__init__(db=db)

        self.bar = OrderedDict()

        self.is_start = False
        self.is_end = False

        self.iter = None

        self.iter_tag = True
        self.day_time = None

        self.end_quot_tag = False

    @staticmethod
    def pre_trade_date(start):
        while True:
            pre_start = start + timedelta(days=-1)
            if fetch.is_trade_date(pre_start):
                return pre_start

    @staticmethod
    def next_trade_date(start):
        while True:
            next_start = start + timedelta(days=1)
            if fetch.is_trade_date(start):
                return next_start

    async def add_code(self, codes) -> bool:
        if len(codes) > 0:
            if not await super().add_code(codes):
                return False
            if self.day_frequency == 0:
                if not await self.init_bar(codes):
                    return False
            else:
                if not await self.init_daily_bar(codes):
                    return False
        return True

    async def init_bar(self, codes) -> bool:
        bar = OrderedDict()
        for code in codes:
            df = fetch.fetch_stock_minute(code=code, period=str(int(self.frequency / 60)),
                                          start=self.start_date, end=self.end_date)
            if df is None:
                self.log.error('指数/股票: {}, 频率: {}min k线无数据'.format(code, int(self.frequency / 60)))
                return False
            df['name'] = self.code_info[code]
            bar[code] = df

        if not await self.add_bar(bars=bar):
            return False

        return True

    async def add_bar(self, bars) -> bool:
        if len(bars) > 0:
            for code, bar_df in bars.items():
                bar_df['trade_date'] = bar_df['day_time'].apply(lambda d: datetime(year=d.year, month=d.month, day=d.day))
                group = bar_df.groupby('trade_date')
                for trade_date in group.groups.keys():
                    df_data = bar_df[bar_df['trade_date'] == trade_date]
                    df_data['day_open'] = df_data.iloc[0]['open']
                    for i in range(df_data.shape[0]):
                        df_data['day_high'] = df_data[:i + 1]['high'].cummax()
                        df_data['day_min'] = df_data[:i + 1]['low'].cummin()

                    for data in df_data.to_dict('records'):
                        day_time = data['day_time']
                        if day_time not in self.bar:
                            self.bar[day_time] = OrderedDict()
                        self.bar[day_time][code] = data

        if self.day_time is None:
            self.iter = iter(self.bar)
        return True

    async def init_daily_bar(self, codes) -> bool:
        if len(codes) > 0:
            df_data = None
            if isinstance(self.db_data, StockDB):
                df_data = await self.db_data.load_stock_daily(
                    filter={'code': {'$in': self.codes},
                            'trade_date': {'$gte': self.start_date, '$lte': self.end_date}},
                    sort=[('trade_date', 1)])
            elif isinstance(self.db_data, FundDB):
                df_data = await self.db_data.load_fund_daily(
                    filter={'code': {'$in': self.codes},
                            'trade_date': {'$gte': self.start_date, '$lte': self.end_date}},
                    sort=[('trade_date', 1)])

            if df_data is None or df_data.empty:
                return False

            df_data['day_high'] = df_data['high']
            df_data['day_min'] = df_data['low']
            df_data['day_open'] = df_data['open']

            for data in df_data.to_dict('records'):
                day_time = datetime.strptime(data['trade_date'].strftime('%Y-%m-%d') + ' 15:00:00', '%Y-%m-%d %H:%M:%S')
                if day_time not in self.bar:
                    self.bar[day_time] = OrderedDict()
                data['name'] = self.code_info[data['code']]
                data['day_time'] = day_time
                self.bar[day_time][data['code']] = data

        if self.day_time is None:
            self.iter = iter(self.bar)
        return True

    async def get_quot(self) -> Optional[Tuple[Optional[str], Optional[Dict]]]:
        now = datetime.now()
        try:
            if not self.is_start:
                self.is_start = True
                return event.evt_start, dict(frequency=self.opt['frequency'],
                                            start=now,
                                            end=now)

            if self.is_start and not self.is_end:
                if self.iter_tag:
                    self.day_time = next(self.iter)

                status_dict = self.get_status_dict(self.day_time)

                quot = self.bar[self.day_time]
                morning_end_date = datetime(year=self.day_time.year, month=self.day_time.month, day=self.day_time.day,
                                            hour=11, minute=30, second=0)
                noon_end_date = datetime(year=self.day_time.year, month=self.day_time.month, day=self.day_time.day,
                                         hour=15, minute=0, second=0)
                if status_dict['evt_morning_start'] and self.day_time == morning_end_date and not self.end_quot_tag:
                    self.iter_tag = False
                    self.end_quot_tag = True
                    return event.evt_quotation, dict(frequency=self.opt['frequency'],
                                                    trade_date=self.trade_date,
                                                    day_time=now,
                                                    list=quot)
                if status_dict['evt_noon_start'] and self.day_time == noon_end_date and not self.end_quot_tag:
                    self.iter_tag = False
                    self.end_quot_tag = True
                    return event.evt_quotation, dict(frequency=self.opt['frequency'],
                                                    trade_date=self.trade_date,
                                                    day_time=now,
                                                    list=quot)
                evt, payload = await self.get_base_event(now=self.day_time)
                if evt is not None:
                    self.iter_tag = False
                    if self.end_quot_tag:
                        self.end_quot_tag = False
                        self.iter_tag = True
                    return evt, payload

                self.iter_tag = True
                self.end_quot_tag = False
                return event.evt_quotation, dict(frequency=self.opt['frequency'],
                                                trade_date=self.trade_date,
                                                day_time=now,
                                                list=quot)

        except StopIteration:
            if not self.is_end:
                self.is_end = True
            return event.evt_end, dict(frequency=self.opt['frequency'],
                                      start=now,
                                      end=now)
        except Exception as e:
            self.log.error('BacktestQuotation get_quot 异常, ex={}, call={}'.format(e, traceback.format_exc()))

        return None, None


class RealtimeQuotation(Quotation):
    def __init__(self, db: MongoDB):
        super().__init__(db=db)

        self.pre_bar = None
        self.bar = None
        self.bar_time = None

        self.last_pub = None

    def pub_bar(self, now, quots):
        self.update_bar(now, quots)

        delta = now - self.bar_time['start']
        if delta.seconds >= self.frequency or self.last_pub is None:
            self.bar_time['end'] = now
            return self.bar
        return None

    def reset_bar(self, now):
        self.pre_bar = self.bar
        self.bar_time = dict(start=None, end=None)
        self.bar = None
        self.last_pub = now

    def update_bar(self, now, quots):
        def _get_quot(pre_bar, q, c, field):
            if pre_bar is None:
                return q[field]
            if c not in pre_bar:
                return q[field]
            return pre_bar[c][field]

        if self.bar is None:
            self.bar = self.pre_bar
            if self.bar is None:
                self.bar = OrderedDict()
            for quot in quots.to_dict('records'):
                code = quot['code']
                if code not in self.codes:
                    print('not in')
                    continue
                self.bar[code] = dict(code=code,
                                      name=self.code_info[code],
                                      day_time=quot['day_time'],
                                      day_open=quot['open'], day_high=quot['high'], day_low=quot['low'],
                                      last_close=quot['last_close'],
                                      open=_get_quot(self.pre_bar, quot, code, 'close'),
                                      high=_get_quot(self.pre_bar, quot, code, 'close'),
                                      low=_get_quot(self.pre_bar, quot, code, 'close'),
                                      close=quot['close'],
                                      volume=_get_quot(self.pre_bar, quot, code, 'volume'),
                                      amount=_get_quot(self.pre_bar, quot, code, 'amount'),
                                      turnover=_get_quot(self.pre_bar, quot, code, 'turnover'), )
            self.bar_time = dict(start=now, end=None)
        else:
            for quot in quots.to_dict('records'):
                bar = self.bar[quot['code']]
                now_price = quot['close']
                bar['close'] = now_price
                if now_price > bar['high']:
                    bar['high'] = now_price
                if now_price < bar['low']:
                    bar['low'] = now_price
                bar['day_high'] = quot['high']
                bar['day_low'] = quot['low']
                bar['day_time'] = quot['day_time']

    async def get_quot(self) -> Optional[Tuple[Optional[str], Optional[Dict]]]:
        try:
            # now = datetime.now()
            now = datetime(year=2020, month=12, day=9, hour=14, minute=0, second=0)
            evt, payload = await self.get_base_event(now=now)
            if evt is not None:
                return evt, payload

            quot = None
            if self.is_trading():
                quots = fetch.fetch_stock_rt_quote(codes=self.codes)
                if quots is not None:
                    quot = self.pub_bar(now, quots)

            if quot is not None:
                self.reset_bar(now)
                return event.evt_quotation, dict(frequency=self.opt['frequency'],
                                                trade_date=self.trade_date,
                                                day_time=now,
                                                list=quot)
        except Exception as e:
            self.log.error('get_quot 异常, ex={}, call={}'.format(e, traceback.format_exc()))

        return None, None


if __name__ == '__main__':
    from bbq.common import run_until_complete
    import asyncio

    db = StockDB()
    db.init()

    rt = RealtimeQuotation(db=db)
    bt = BacktestQuotation(db=db)


    async def test_rt():
        if not await rt.init(opt=dict(frequency='10s', codes=['sh601099', 'sz000001'])):
            print('初始化失败')
            return
        i = 0
        while True:
            data = await rt.get_quot()
            print('data: {}'.format(data))
            await asyncio.sleep(1)
            if i == 3:
                await rt.add_code(['sz300076'])
            i += 1


    async def test_bt():
        if not await bt.init(opt=dict(frequency='60min', codes=['sz000001', 'sh601099'],
                                      start_date=datetime.strptime('2020-12-01', '%Y-%m-%d'),
                                      end_date=datetime.strptime('2020-12-04', '%Y-%m-%d'))):
            print('初始化失败')
            return
        i = 0
        while True:
            data = await bt.get_quot()
            if data[0] is None:
                break
            if i == 7:
                await bt.add_code(['sz300076'])
            print('data: {}'.format(data))
            i += 1
            # await asyncio.sleep(5)


    run_until_complete(
        test_rt()
        # test_bt()
    )
