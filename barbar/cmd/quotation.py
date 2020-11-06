from barbar.stock_nats import StockNats
import asyncio
import os
import barbar.log as log
import traceback
from barbar.config import conf_dict
import click
from barbar.data.stockdb import StockDB
from collections import OrderedDict
import uuid
from typing import Dict
import barbar.fetch as fetch
from datetime import datetime, timedelta
from collections import defaultdict

"""
监听topic: topic:barbar.quotation.command
提供功能: 1. 订阅行情 2.取消订阅

订阅行情时, 返回 topic:barbar.quotation/backtest.realtime. + str(uuid.uuid4()) 两种topic给订阅者，
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
        "subject": "topic:barbar.quotation.xxxxxxxx"
    }
}

1.2 订阅行情
req:
{
  "cmd": "unsubscribe",
  "data": {
    "subject": "topic:barbar.quotation.xxxxxxxx"
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


class BacktestQuotation:
    def __init__(self, loop, subject: str, nats: StockNats, db: StockDB, data: Dict):
        self.log = log.get_logger(self.__class__.__name__)

        self.subject = subject
        self.nats = nats
        self.data = data

        self.running = False

        self.db = db
        self.loop = loop

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

    def stop(self) -> None:
        self.running = False

    async def init(self):
        try:
            start, end, frequency = self.data['start'], self.data['end'], self.data['frequency'].lower()
            index_list, stock_list = self.data['index_list'], self.data['stock_list']
            if 'min' not in frequency:
                self.log.error('frequency 格式不正确')
                return False, 'frequency 格式不正确'

            if frequency not in ['1min', '5min', '15min', '30min', '60min']:
                self.log.error('frequency 格式不正确')
                return False, 'frequency 格式不正确'

            start = datetime.strptime(start, '%Y-%m-%d %H:%M:%S').strftime('%Y%m%d %H:%M:%S')
            end = datetime.strptime(end, '%Y-%m-%d %H:%M:%S').strftime('%Y%m%d %H:%M:%S')

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

    async def quot_task(self):
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

    async def start(self):
        self.running = True
        self.loop.create_task(self.quot_task())


class RealtimeQuotation:
    def __init__(self, loop, subject: str, nats: StockNats, db: StockDB, data: Dict):
        self.log = log.get_logger(self.__class__.__name__)

        self.subject = subject
        self.nats = nats
        self.data = data

        self.running = False

        self.frequency = 0
        self.codes = None

        self.quot_date = {}

        self.db = db
        self.loop = loop

        self.bar = None
        self.bar_time = None

        self.last_pub = None

    def stop(self) -> None:
        self.running = False

    async def init(self):
        try:
            frequency, index_list, stock_list = self.data['frequency'].lower(), self.data['index_list'], self.data[
                'stock_list']
            if 'min' not in frequency:
                self.log.error('frequency 格式不正确')
                return False, 'frequency 格式不正确'
            # 最低1分钟
            frequency = int(frequency.split('min')[0])
            if frequency <= 0:
                self.log.error('frequency 格式不正确')
                return False, 'frequency 格式不正确'

            self.frequency = frequency * 60
            self.codes = index_list + stock_list

            return True, ''

        except Exception as e:
            self.log.error('realtime quot 初始化失败, ex={}, callstack={}'.format(e, traceback.format_exc()))
            return False, 'realtime quot 初始化失败'

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
                trade_dates = await self.db.load_trade_cal(filter={'cal_date': date_now, 'is_open': 1})
                is_open = False
                if trade_dates is not None and not trade_dates.empty:
                    is_open = True

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

    async def start(self):
        self.running = True
        self.loop.create_task(self.quot_task())


class Quotation(StockNats):

    def __init__(self, options: Dict, db: StockDB):
        self.loop = asyncio.get_event_loop()
        super().__init__(loop=self.loop, options=options)

        self.db = db

        self.topic_cmd = 'topic:barbar.quotation.command'

        self.cmd_handlers = {
            'subscribe': self.on_subscribe,
            'unsubscribe': self.on_unsubscribe
        }
        self.add_handler(self.topic_cmd, self.cmd_handlers)

        self.subject = {}

    def start(self):
        try:
            if self.loop.run_until_complete(self.nats_task()):
                self.loop.create_task(self.heartbeat_task())
                self.loop.create_task(self.subscribe(self.topic_cmd))
                self.loop.run_forever()
        except Exception as e:
            self.log.error('quotation start 异常, ex={}, callstack={}'.format(e, traceback.format_exc()))
        finally:
            self.loop.close()

    async def heartbeat_task(self):
        # timeout_dict = defaultdict(int)
        while self.loop.is_running():
            invalid_list = []
            for subject, quot in self.subject.items():
                try:
                    rsp = await self.request(subject=subject, data=dict({'cmd': 'heartbeat'}), timeout=15)
                    # self.log.debug('subject: {}, heartbeat: {}'.format(subject, rsp))
                    # timeout_dict[subject] = 0
                except Exception as e:
                    self.log.error('心跳异常: {}'.format(e))
                    if quot is not None:
                        quot.stop()
                    invalid_list.append(subject)
                    # timeout_dict[subject] += 1
                    # if subject in timeout_dict:
                    #     if timeout_dict[subject] >= 3:
                    #         self.log.error('心跳异常次数{}, 大于3, 停止下发行情')
                    #         if quot is not None:
                    #             quot.stop()
                    #         invalid_list.append(subject)
            for subject in invalid_list:
                del self.subject[subject]

            await asyncio.sleep(delay=5, loop=self.loop)

    async def on_unsubscribe(self, data):
        self.log.info('on_unsubscribe: {}'.format(data))
        subject = data['subject']
        if subject in self.subject:
            quot = self.subject[subject]
            if quot is not None:
                quot.stop()
            del self.subject[subject]

        return 'OK', 'SUCCESS', dict(subject=subject)

    async def on_subscribe(self, data):
        self.log.info('on_subscribe: {}'.format(data))
        typ = data['type'].lower()

        if typ == 'realtime' or typ == 'simulate':
            subject = 'topic:barbar.quotation.realtime.' + str(uuid.uuid4()).replace('-', '')
            rt = RealtimeQuotation(loop=self.loop, subject=subject, db=self.db, nats=self, data=data)
            suc, desc = await rt.init()
            if not suc:
                return 'FAIL', desc
            
            self.loop.create_task(rt.start())
            self.subject[subject] = rt

            return 'OK', 'SUCCESS', dict(subject=subject)

        if typ == 'backtest':
            subject = 'topic:barbar.quotation.backtest.' + str(uuid.uuid4()).replace('-', '')
            bt = BacktestQuotation(loop=self.loop, subject=subject, db=self.db, nats=self, data=data)
            suc, desc = await bt.init()
            if not suc:
                return 'FAIL', desc
            self.loop.create_task(bt.start())
            self.subject[subject] = bt

            return 'OK', 'SUCCESS', dict(subject=subject)

        return 'FAIL', '未知类型'


@click.command()
@click.option('--uri', type=str, help='mongodb connection uri')
@click.option('--pool', default=0, type=int, help='mongodb connection pool size')
@click.option('--nats', type=str, help='mongodb uri')
@click.option('--debug/--no-debug', default=True, help='show debug log')
def main(uri: str, pool: int, nats: str, debug: bool):
    uri = conf_dict['mongo']['uri'] if uri is None else uri
    pool = conf_dict['mongo']['pool'] if pool <= 0 else pool
    nats = conf_dict['nats']['uri'] if nats is None else nats

    file = None
    level = "critical"
    if debug:
        file = conf_dict['log']['path'] + os.sep + 'fund_sync.log'
        level = conf_dict['log']['level']

    log.setup_logger(file=file, level=level)
    logger = log.get_logger()

    logger.debug('初始化数据库')
    db = StockDB(uri=uri, pool=pool)
    if not db.init():
        logger.error('初始化数据库失败')
        return

    fetch.init()

    quot = Quotation(options={'servers': [nats]}, db=db)
    quot.start()


if __name__ == '__main__':
    main()
