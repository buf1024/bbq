import asyncio
from abc import ABC
import traceback
import uuid
from collections import OrderedDict
from datetime import datetime
from typing import Dict, Optional
import bbq.fetch as fetch
import bbq.log as log
from bbq.data.mongodb import MongoDB

"""
监听topic: topic:bbq.quotation.command
提供功能: 1. 订阅行情 2.取消订阅

订阅行情时, 返回 topic:bbq.quotation/backtest.realtime. + str(uuid.uuid4()) 两种topic给订阅者，
订阅者成功收到topic后，则可订阅该topic下发的行情。行情下发顺序：
start -> trade_morning_start -> trade_morning_end -> trade_noon_start -> trade_noon_end -> end
取消订阅行情时，发送对应的topic则可以取消该行情的订阅。

行情程序定时检测订阅者是否有效，如无效则不下发行情，甚至退出该订阅的订阅。
统一订阅者如没收到下发的程序，可以重复进行订阅。

交互协议:

1.1 订阅行情
req:
{
  "cmd": "subscribe",
  "data": {
    "type": "realtime" | "backtest",
    "frequency": "1min", "5min", '15min', '30min', '60min',
    "start": yyyy-mm-dd hh:mm:ss, backtest
    "end": yyyy-mm-dd hh:mm:ss, backtest
    "index_list": ["000001.SH"],
    "stock_list": ["000001.SZ"]
  }
}
resp:
{
    "status": 'OK',
    "msg": "SUCCESS",
    "data": {
        "subject": "topic:bbq.quotation.xxxxxxxx"
    }
}

1.2 订阅行情
req:
{
  "cmd": "unsubscribe",
  "data": {
    "subject": "topic:bbq.quotation.xxxxxxxx"
  }
}

2.1 行情下发
{
    "cmd": "quotation",
    "data": {
        "type": "start" "trade_morning_start", "trade_morning_end", "trade_noon_start", "trade_noon_end"| "end" | "quot"
        "frequency": "1min", "5min", '15min', '30min', '60min',
        "start": "yyyymmdd hhmmss"
        "end": "yyyymmdd hhmmss"
        "list": {
            "000001.SZ": {}
        }
    }
}
"""


class Quotation(ABC):
    def __init__(self, db: MongoDB):
        self.log = log.get_logger(self.__class__.__name__)
        self.db_data = db
        self.opt = None

        self.frequency = 0
        self.index = []
        self.stock = []
        self.start_date = None
        self.end_date = None

        self.quot_date = {}

    async def init(self, opt) -> bool:
        self.opt = opt
        try:
            frequency, index, stock = self.opt['frequency'].lower(), self.opt['index'], self.opt['stock']
            if 'min' not in frequency:
                self.log.error('frequency 格式不正确')
                return False
            # 最低1分钟
            frequency = int(frequency.split('min')[0])
            if frequency <= 0:
                self.log.error('frequency 格式不正确')
                return False

            self.frequency = frequency * 60

            if 'start_date' in self.opt and self.opt['start_date'] is not None:
                self.start_date = datetime.strptime(self.opt['start_date'], 'yyyymmdd')

            if 'end_date' in self.opt and self.opt['end_date'] is not None:
                self.end_date = datetime.strptime(self.opt['end_date'], 'yyyymmdd')

            return True

        except Exception as e:
            self.log.error('realtime quot 初始化失败, ex={}'.format(e))
            return False

    async def get_quot(self) -> Optional[Dict]:
        return None


class BacktestQuotation(Quotation):
    def __init__(self, db: MongoDB):
        super().__init__(db=db)

        self.bar = OrderedDict()

        self.quot_date = {}

    def add_bar(self, bars):
        if len(bars) > 0:
            for code, bar_df in bars.items():
                for item in bar_df.to_dict('records'):
                    dt = item['datetime']
                    item['datetime'] = item['datetime'].strftime('%Y-%m-%d %H:%M:%S')
                    item['trade_date'] = item['trade_date'].strftime('%Y-%m-%d')
                    if dt not in self.bar:
                        self.bar[dt] = OrderedDict()
                    self.bar[dt][item['code']] = item

    async def init(self, opt):
        try:

            if not await super().init(opt=opt):
                return False

            bar_index = OrderedDict()
            if index_list is not None:
                for index in index_list:
                    df = fetch.get_index_bar(code=index, frequency=frequency,
                                             start=start, end=end)
                    if df is None:
                        self.log.error('指数{}, {} k线无数据'.format(index, frequency))
                        return False, '指数{}, {} k线无数据'.format(index, frequency)
                    bar_index[index] = df

            bar_stock = OrderedDict()
            if stock_list is not None:
                for code in stock_list:
                    df = fetch.get_bar(code=code, frequency=frequency,
                                       start=start, end=end)
                    if df is None:
                        self.log.error('股票{}, {} k线无数据'.format(code, frequency))
                        return False, '股票{}, {} k线无数据'.format(code, frequency)
                    bar_stock[code] = df

            self.add_bar(bars=bar_stock)
            self.add_bar(bars=bar_index)

            return True, ''

        except Exception as e:
            self.log.error('backtest quot 初始化失败, ex={}, callstack={}'.format(e, traceback.format_exc()))
            return False, 'backtest quot 初始化失败'

    async def get_quot(self):
        self.log.info('start backtest quot task: {}'.format(self.subject))
        # backtest等订阅者ready
        await asyncio.sleep(delay=1, loop=self.loop)

        now = datetime.now()
        await self.nats.publish(self.subject, dict(cmd='quotation',
                                                   data=dict(type='start',
                                                             frequency=self.data['frequency'],
                                                             start=now.strftime('%Y-%m-%d %H:%M:%S'),
                                                             end=now.strftime('%Y-%m-%d %H:%M:%S'))))
        for now, bars in self.bar.items():
            if not self.running:
                break

            date_now = datetime(year=now.year, month=now.month, day=now.day)
            if date_now not in self.quot_date:
                self.quot_date.clear()
                self.quot_date[date_now] = dict(is_open=True,
                                                is_morning_start=False,
                                                is_morning_end=False,
                                                is_noon_start=False,
                                                is_noon_end=False)
            status_dict = self.quot_date[date_now]

            morning_start_date = datetime(year=now.year, month=now.month, day=now.day, hour=9, minute=30, second=0)
            morning_end_date = datetime(year=now.year, month=now.month, day=now.day, hour=11, minute=30, second=0)

            noon_start_date = datetime(year=now.year, month=now.month, day=now.day, hour=13, minute=0, second=0)
            noon_end_date = datetime(year=now.year, month=now.month, day=now.day, hour=15, minute=0, second=0)

            if morning_start_date <= now < morning_end_date:
                if not status_dict['is_morning_start']:
                    await self.nats.publish(self.subject, dict(cmd='quotation',
                                                               data=dict(type='trade_morning_start',
                                                                         frequency=self.data['frequency'],
                                                                         start=now.strftime(
                                                                             '%Y-%m-%d %H:%M:%S'),
                                                                         end=now.strftime(
                                                                             '%Y-%m-%d %H:%M:%S'))))
                    status_dict['is_noon_start'] = True
            elif morning_end_date <= now < noon_start_date:
                if not status_dict['is_morning_end']:
                    await self.nats.publish(self.subject, dict(cmd='quotation',
                                                               data=dict(type='trade_morning_end',
                                                                         frequency=self.data['frequency'],
                                                                         start=now.strftime(
                                                                             '%Y-%m-%d %H:%M:%S'),
                                                                         end=now.strftime(
                                                                             '%Y-%m-%d %H:%M:%S'))))
                    status_dict['is_morning_end'] = True
            elif noon_start_date <= now < noon_end_date:
                if not status_dict['is_noon_start']:
                    await self.nats.publish(self.subject, dict(cmd='quotation',
                                                               data=dict(type='trade_noon_start',
                                                                         frequency=self.data['frequency'],
                                                                         start=now.strftime(
                                                                             '%Y-%m-%d %H:%M:%S'),
                                                                         end=now.strftime(
                                                                             '%Y-%m-%d %H:%M:%S'))))
                    status_dict['is_noon_start'] = True
            elif now >= noon_end_date:
                if not status_dict['is_noon_end']:
                    await self.nats.publish(self.subject, dict(cmd='quotation',
                                                               data=dict(type='trade_noon_end',
                                                                         frequency=self.data['frequency'],
                                                                         start=now.strftime('%Y-%m-%d %H:%M:%S'),
                                                                         end=now.strftime('%Y-%m-%d %H:%M:%S'))))
                    status_dict['is_noon_end'] = True

            await self.nats.publish(self.subject,
                                    dict(cmd='quotation',
                                         data=dict(type='quot',
                                                   frequency=self.data['frequency'],
                                                   start=now.strftime('%Y-%m-%d %H:%M:%S'),
                                                   end=now.strftime('%Y-%m-%d %H:%M:%S'),
                                                   list=bars)))

        now = datetime.now()
        await self.nats.publish(self.subject, dict(cmd='quotation',
                                                   data=dict(type='end',
                                                             frequency=self.data['frequency'],
                                                             start=now.strftime('%Y-%m-%d %H:%M:%S'),
                                                             end=now.strftime('%Y-%m-%d %H:%M:%S'))))

        self.log.info('end backtest quot task: {}'.format(self.subject))


class RealtimeQuotation(Quotation):
    def __init__(self, db: MongoDB):
        super().__init__(db=db)

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

    def update_bar(self, now, quots):
        if self.bar is None:
            self.bar = {}
            for code, quot in quots.items():
                self.bar[code] = dict(code=quot['code'], name=quot['name'],
                                      trade_date=quot['date'].strftime('%Y-%m-%d'),
                                      open=quot['now'], high=quot['high'], low=quot['low'], close=quot['now'],
                                      vol=quot['vol'], amount=quot['amount'])
            self.bar_time = dict(start=now)
        else:
            for code, quot in quots.items():
                bar = self.bar[code]
                bar['close'] = quot['now']
                if quot['high'] > bar['high']:
                    bar['high'] = quot['high']
                if quot['low'] < bar['low']:
                    bar['low'] = quot['low']
                bar['datetime'] = quot['datetime'].strftime('%Y-%m-%d %H:%M:%S')

    async def get_quot(self):
        try:
            now = datetime.now()
            date_now = datetime(year=now.year, month=now.month, day=now.day)
            if len(self.quot_date) == 0 or date_now not in self.quot_date:
                self.quot_date.clear()
                is_open = fetch.is_trade_date(date_now)

                self.quot_date[date_now] = dict(is_open=is_open,
                                                is_morning_start=False,
                                                is_morning_end=False,
                                                is_noon_start=False,
                                                is_noon_end=False)

            status_dict = self.quot_date[date_now]

            if not status_dict['is_open']:
                return

            quot = None

            morning_start_date = datetime(year=now.year, month=now.month, day=now.day, hour=9, minute=30, second=0)
            morning_end_date = datetime(year=now.year, month=now.month, day=now.day, hour=11, minute=30, second=0)

            noon_start_date = datetime(year=now.year, month=now.month, day=now.day, hour=13, minute=0, second=0)
            noon_end_date = datetime(year=now.year, month=now.month, day=now.day, hour=15, minute=0, second=0)

            if morning_start_date <= now < morning_end_date:
                if not status_dict['is_morning_start']:
                    await self.nats.publish(self.subject, dict(cmd='quotation',
                                                               data=dict(type='trade_morning_start',
                                                                         frequency=self.data['frequency'],
                                                                         start=now.strftime('%Y-%m-%d %H:%M:%S'),
                                                                         end=now.strftime('%Y-%m-%d %H:%M:%S'))))
                    status_dict['is_morning_start'] = True
                quots = await fetch.get_rt_quot(codes=self.codes)
                quot = self.pub_bar(now, quots)
            elif morning_end_date <= now < noon_start_date:
                if not status_dict['is_morning_end']:
                    await self.nats.publish(self.subject, dict(cmd='quotation',
                                                               data=dict(type='trade_morning_end',
                                                                         frequency=self.data['frequency'],
                                                                         start=now.strftime(
                                                                             '%Y-%m-%d %H:%M:%S'),
                                                                         end=now.strftime(
                                                                             '%Y-%m-%d %H:%M:%S'))))
                    status_dict['is_morning_end'] = True
            elif noon_start_date <= now < noon_end_date:
                if not status_dict['is_noon_start']:
                    await self.nats.publish(self.subject, dict(cmd='quotation',
                                                               data=dict(type='trade_noon_start',
                                                                         frequency=self.data['frequency'],
                                                                         start=now.strftime(
                                                                             '%Y-%m-%d %H:%M:%S'),
                                                                         end=now.strftime(
                                                                             '%Y-%m-%d %H:%M:%S'))))
                    status_dict['is_noon_start'] = True
                quots = await fetch.get_rt_quot(codes=self.codes)
                quot = self.pub_bar(now, quots)
            elif now >= noon_end_date:
                if not status_dict['is_noon_end']:
                    await self.nats.publish(self.subject, dict(cmd='quotation',
                                                               data=dict(type='trade_noon_end',
                                                                         frequency=self.data['frequency'],
                                                                         start=now.strftime('%Y-%m-%d %H:%M:%S'),
                                                                         end=now.strftime('%Y-%m-%d %H:%M:%S'))))
                    status_dict['is_noon_end'] = True

            if quot is not None:
                await self.nats.publish(self.subject,
                                        dict(cmd='quotation',
                                             data=dict(type='quot',
                                                       frequency=self.data['frequency'],
                                                       start=self.bar_time['start'].strftime('%Y-%m-%d %H:%M:%S'),
                                                       end=self.bar_time['end'].strftime('%Y-%m-%d %H:%M:%S'),
                                                       list=quot)))
                self.bar = None
                self.bar_time = None
                self.last_pub = now
        except Exception as e:
            self.log.error('get_quot 异常, ex={}, callstack={}'.format(e, traceback.format_exc()))

    async def quot_task(self):
        # 等订阅者ready
        await asyncio.sleep(delay=1, loop=self.loop)

        now = datetime.now()
        await self.nats.publish(self.subject, dict(cmd='quotation',
                                                   data=dict(type='start',
                                                             frequency=self.data['frequency'],
                                                             start=now.strftime('%Y-%m-%d %H:%M:%S'),
                                                             end=now.strftime('%Y-%m-%d %H:%M:%S'))))
        while self.running:
            await self.get_quot()
            await asyncio.sleep(delay=1, loop=self.loop)

        now = datetime.now()
        await self.nats.publish(self.subject, dict(cmd='quotation',
                                                   data=dict(type='end',
                                                             frequency=self.data['frequency'],
                                                             start=now.strftime('%Y-%m-%d %H:%M:%S'),
                                                             end=now.strftime('%Y-%m-%d %H:%M:%S'))))

