from barbar.async_nats import AsyncNats
import json
import os
import signal
import traceback
import barbar.log as log
import asyncio
from barbar.config import conf_dict
import click
from barbar.data.stockdb import StockDB
from threading import Thread
from concurrent.futures import ThreadPoolExecutor

# 监听 topic:barbar.quotation.command
"""
1.1 订阅行情
req:
{
  "cmd": "subscribe",
  "data": {
    "type": "realtime" | "backtest",
    "frequency": "5min",
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
        "type": "start" | "end" | "quot"
        "frequency": "5min",
        "start": "yyyymmdd hhmmss"
        "end": "yyyymmdd hhmmss"
        "list": {
            "000001.SZ": {}
        }
    }
}
"""


class Quotation(AsyncNats):

    def __init__(self, loop, db):
        super().__init__(loop=loop)

        self.db = None

        self.topic_cmd = 'topic:barbar.quotation.command'

        self.cmd_handlers = {
            'subscribe': self.on_subscribe,
            'unsubscribe': self.on_unsubscribe
        }
        self.subject = []

    def start(self):
        # self.db = SourceMongo()
        # if not self.db.init(self.db_opt):
        #     self.log.error('connect db error')
        #     return
        # for sig in ('SIGINT', 'SIGTERM'):
        #     self.loop.add_signal_handler(getattr(signal, sig), self.on_signal
        super().start()

    async def on_unsubscribe(self, subject, data):
        self.log('on_unsubscribe: {} {}'.format(subject, data))

    async def my_task(self, nc):
        js = json.dumps(dict(cmd="unsubscribe", data="data"))
        self.log.info("publish: {}".format(js))
        a = await nc.publish(subject="hello:world", payload=js.encode())
        print(a)

    def my_th(self, nc):
        self.log.info('nc thread')
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            loop.run_until_complete(self.my_task(nc))
        finally:
            loop.close()

    async def on_subscribe(self, subject, data):
        self.log.info('on_subscribe: {} {}'.format(subject, data))

        Thread(target=self.my_th, args=(self.nc, )).start()

        return "ok", 'succe', ''

    async def on_nats_connected(self):
        self.log.info('nats connected, listen subject: {}'.format(self.topic_cmd))
        await self.subscribe(self.topic_cmd)

    async def on_nats_message(self, msg):
        subject, reply, data = msg.subject, msg.reply, msg.data.decode()
        self.log.info("Received a message on '{subject}': {data}".format(
            subject=subject, reply=reply, data=data))

        data = json.loads(data)
        handlers = None
        if subject == self.topic_cmd:
            handlers = self.cmd_handlers
        if handlers is not None:
            await self.on_command(handlers=handlers, subject=subject, reply=reply, data=data)

    async def on_command(self, handlers, subject, reply, data):
        resp = None
        try:
            cmd, arg = data['cmd'], data['data'] if 'data' in data else None
            status, msg, data = await handlers[cmd](subject, data)
            resp = dict(status=status, msg=msg, data=data)
        except Exception as e:
            self.log.error(
                'execute command failed: exception={} subject={}, data={} call_stack={}'.format(e, subject, data,
                                                                                                traceback.format_exc()))
            resp = dict(status='FAIL', msg='调用异常', data=None)
        finally:
            if reply != '':
                js_resp = json.dumps(resp)
                self.log.info("Response a message: '{data}'".format(data=js_resp))
                await self.nc.publish(subject=reply, payload=js_resp.encode())
                await self.nc.flush()


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

    loop = None
    try:
        loop = asyncio.get_event_loop()
        quot = Quotation(loop=loop, db=db)
        for sig in ('SIGINT', 'SIGTERM'):
            loop.add_signal_handler(getattr(signal, sig), quot.on_signal)

        if loop.run_until_complete(quot.task({'servers': [nats]})):
            loop.run_forever()
        else:
            logger.error('连接nats失败, uri:{}'.format(nats))

    finally:
        if loop is not None:
            loop.close()


if __name__ == '__main__':
    main()
