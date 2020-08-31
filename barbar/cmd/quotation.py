from barbar.async_nats import AsyncNats
import asyncio
import json
import os
import barbar.log as log
import traceback
from barbar.config import conf_dict
import click
from barbar.data.stockdb import StockDB
from threading import Thread
import time
import uuid
from typing import Dict
import barbar.fetch as fetch
from datetime import datetime, timedelta

# 监听 topic:barbar.quotation.command
"""
1.1 订阅行情
req:
{
  "cmd": "subscribe",
  "data": {
    "type": "realtime" | "backtest",
    "frequency": "1min", "5min", '15min', '30min', '60min',
    "index_list": ["000001.SH"],
    "stock_list": ["000001.SZ"]
  }
}
resp:
{
    "status": "OK",
    "msg": "SUCCESS",
    "data": {
        "queue": "topic:barbar.quotation.xxxxxxxx"
    }
}

1.2 订阅行情
req:
{
  "cmd": "unsubscribe",
  "data": {
    "queue": "topic:barbar.quotation.xxxxxxxx"
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


class RealtimeQuotation(Thread):
    def __init__(self, subject: str, nats: AsyncNats, db_uri: str, db_pool: int, data: Dict):
        super().__init__(daemon=True)
        self.log = log.get_logger(self.__class__.__name__)

        self.subject = subject
        self.nats = nats
        self.data = data

        self.running = False
        self.loop = None

        self.frequency = 0
        self.codes = None

        self.quot_date = {}

        self.last_pub = None

        self.db = StockDB(uri=db_uri, pool=db_pool)

    def stop(self) -> None:
        self.running = False

    async def init(self):
        try:
            if not self.db.init():
                self.log.error('连接数据库错误')
                return False

            frequency, index_list, stock_list = self.data['frequency'].lower(), self.data['index_list'], self.data[
                'stock_list']
            if 'min' not in frequency:
                self.log.error('frequency 格式不正确')
                return False
            # 最低1分钟
            frequency = int(frequency.split('min')[0])
            if frequency <= 0:
                self.log.error('frequency 格式不正确')
                return False

            self.frequency = frequency * 60
            self.codes = index_list + stock_list

            return True

        except Exception as e:
            self.log.error('realtime quot 初始化失败, ex={}, callstack={}'.format(e, traceback.format_exc()))
            return False

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
                                                is_nood_end=False)

            status_dict = self.quot_date[date_now]

            if not status_dict['is_open']:
                return

            morning_start_date = datetime(year=now.year, month=now.month, day=now.day, hour=9, minute=30, second=0)
            morning_end_date = datetime(year=now.year, month=now.month, day=now.day, hour=11, minute=30, second=0)

            noon_start_date = datetime(year=now.year, month=now.month, day=now.day, hour=13, minute=0, second=0)
            noon_end_date = datetime(year=now.year, month=now.month, day=now.day, hour=18, minute=0, second=0)

            quot = None
            if morning_start_date <= now < morning_end_date:
                if not status_dict['is_morning_start']:
                    self.nats.publish(self.subject, dict(cmd='quotation',
                                                         data=dict(type='trade_morning_start',
                                                                   frequency=self.data['frequency'],
                                                                   start=now.strftime(
                                                                       '%Y%m%d %H:%M:%S'),
                                                                   end=now.strftime(
                                                                       '%Y%m%d %H:%M:%S'))))
                    status_dict['is_noon_start'] = True
                qf = fetch.get_rt_quot(codes=self.codes)
            elif morning_end_date <= now < noon_start_date:
                if not status_dict['is_morning_end']:
                    self.nats.publish(self.subject, dict(cmd='quotation',
                                                         data=dict(type='trade_morning_end',
                                                                   frequency=self.data['frequency'],
                                                                   start=now.strftime(
                                                                       '%Y%m%d %H:%M:%S'),
                                                                   end=now.strftime(
                                                                       '%Y%m%d %H:%M:%S'))))
                    status_dict['is_morning_end'] = True
            elif noon_start_date <= now < noon_end_date:
                if not status_dict['is_noon_start']:
                    self.nats.publish(self.subject, dict(cmd='quotation',
                                                         data=dict(type='trade_noon_start',
                                                                   frequency=self.data['frequency'],
                                                                   start=now.strftime(
                                                                       '%Y%m%d %H:%M:%S'),
                                                                   end=now.strftime(
                                                                       '%Y%m%d %H:%M:%S'))))
                    status_dict['is_noon_start'] = True
                qf = fetch.get_rt_quot(codes=self.codes)
            elif now >= noon_end_date:
                if not status_dict['is_noon_end']:
                    self.nats.publish(self.subject, dict(cmd='quotation',
                                                         data=dict(type='trade_noon_end',
                                                                   frequency=self.data['frequency'],
                                                                   start=now.strftime('%Y%m%d %H:%M:%S'),
                                                                   end=now.strftime('%Y%m%d %H:%M:%S'))))
                    status_dict['is_noon_end'] = True

            if quot is not None:
                if self.last_pub is None:
                    self.nats.publish(self.subject, dict(cmd='quotation',
                                                         data=dict(type='trade_noon_end',
                                                                   frequency=self.data['frequency'],
                                                                   start=now.strftime('%Y%m%d %H:%M:%S'),
                                                                   end=now.strftime('%Y%m%d %H:%M:%S'),
                                                                   list=quot)))


        except Exception as e:
            self.log.error('get_quot 异常, ex={}, callstack={}'.format(e, traceback.format_exc()))

    async def quot_task(self):
        queue = asyncio.Queue()
        now = datetime.now()
        self.nats.publish(self.subject, dict(cmd='quotation',
                                             data=dict(type='start',
                                                       frequency=self.data['frequency'],
                                                       start=now.strftime('%Y%m%d %H:%M:%S'),
                                                       end=now.strftime('%Y%m%d %H:%M:%S'))))
        while self.running:
            try:
                queue.get_nowait()
            except asyncio.QueueEmpty:
                await self.get_quot()
                await asyncio.sleep(delay=1, loop=self.loop)

        now = datetime.now()
        self.nats.publish(self.subject, dict(cmd='quotation',
                                             data=dict(type='end',
                                                       frequency=self.data['frequency'],
                                                       start=now.strftime('%Y%m%d %H:%M:%S'),
                                                       end=now.strftime('%Y%m%d %H:%M:%S'))))
        self.loop.stop()

    def run(self) -> None:
        try:

            self.loop = asyncio.new_event_loop()
            asyncio.set_event_loop(self.loop)

            if self.loop.run_until_complete(self.init()):
                self.running = True
                self.loop.create_task(self.quot_task())
                self.loop.run_forever()
        finally:
            self.log.info('RealtimeQuotation loop done')


class Quotation:

    def __init__(self, db: StockDB, nats: AsyncNats):
        self.log = log.get_logger(self.__class__.__name__)

        self.db = db
        self.nats = nats

        self.topic_cmd = 'topic:barbar.quotation.command'

        self.cmd_handlers = {
            'subscribe': self.on_subscribe,
            'unsubscribe': self.on_unsubscribe
        }
        self.subject = {}

    def start(self):
        self.nats.start()
        time.sleep(1)

        self.nats.subscribe(self.topic_cmd)
        while True:
            try:
                data = self.nats.queue_out.get_nowait()
                subject, reply, data = data['subject'], data['reply'], data['data']
                handlers = None
                if subject == self.topic_cmd:
                    handlers = self.cmd_handlers

                if handlers is not None:
                    self.on_command(handlers=handlers, subject=subject, reply=reply, data=data)
            except asyncio.QueueEmpty:
                time.sleep(1)
            except Exception as e:
                self.log.error(
                    'wait queue exception={} call_stack={}'.format(e, traceback.format_exc()))

    def on_unsubscribe(self, data):
        self.log('on_unsubscribe: {}'.format(data))

    # def my_task(self, nc):
    #     js = json.dumps(dict(cmd="unsubscribe", data="data"))
    #     self.log.info("publish: {}".format(js))
    #     a = await nc.publish(subject="hello:world", payload=js.encode())
    #     print(a)

    def my_th(self):
        self.log.info('nc thread')
        self.nats.publish(subject="hello:world", data='nice work')

    def on_subscribe(self, data):
        self.log.info('on_subscribe: {}'.format(data))
        typ = data['type']

        if typ == 'realtime':
            # subject = 'topic:barbar.realtime.' + str(uuid.uuid4())
            subject = 'topic:barbar.realtime.abc'
            subject_thread = RealtimeQuotation(subject=subject, db_uri=self.db.uri, db_pool=self.db.pool,
                                               nats=self.nats, data=data).start()
            self.subject[subject] = subject_thread

            return "OK", 'SUCCESS', dict(subject=subject)

        return "FAIL", "未知类型", None

    def on_command(self, handlers, subject, reply, data):
        resp = None
        try:
            cmd, data = data['cmd'], data['data'] if 'data' in data else None
            if cmd in handlers.keys():
                status, msg, data = handlers[cmd](data)
                resp = dict(status=status, msg=msg, data=data)
            else:
                self.log.error('handler not found, data={}'.format(data))
        except Exception as e:
            self.log.error(
                'execute command failed: exception={} subject={}, data={} call_stack={}'.format(e, subject, data,
                                                                                                traceback.format_exc()))
            resp = dict(status='FAIL', msg='调用异常', data=None)
        finally:
            if reply != '' and resp is not None:
                js_resp = json.dumps(resp)
                self.log.info("Response a message: '{data}'".format(data=js_resp))
                self.nats.publish(subject=reply, data=js_resp.encode())


@click.command()
@click.option('--uri', type=str, help='mongodb connection uri')
@click.option('--pool', default=0, type=int, help='mongodb connection pool size')
@click.option('--nats', type=str, help='mongodb connection pool size')
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

    nats = AsyncNats(options={'servers': [nats]})
    quot = Quotation(nats=nats, db=db)
    quot.start()


if __name__ == '__main__':
    main()
