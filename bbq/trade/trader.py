import bbq.log as log
import click
from bbq.config import conf_dict
import os
import uuid
import bbq.fetch as fetch
from bbq.data.stockdb import StockDB
from bbq.stock_nats import StockNats
import asyncio
from multiprocessing import Process
import multiprocessing as mp
from typing import Dict, ClassVar
import traceback
import json

# from bbq.trade.account import Account

"""
监听topic: topic:bbq.trader.command
提供功能: 1. 实盘或模拟盘交易 2.交易回测 3. 停止

采用多进程交易或回测:
Trader
trade/backtest -> TradeReal/TradeBacktest(topic:bbq.trader.trade.command.{}.format(uuid.uuid4()))启动进程

TradeReal/TradeBacktest
订阅行情 -> 监听行情推送 -> 处理行情
订阅其他命令

1. 
{

"""


def run(cls: ClassVar, **kwargs):
    """

    :param cls:
    :param kwargs:
    :return:
    """

    uri = conf_dict['mongo']['uri'] if 'uri' not in kwargs or kwargs['uri'] is None else kwargs['uri']
    pool = conf_dict['mongo']['pool'] if 'pool' not in kwargs or kwargs['pool'] <= 0 else kwargs['pool']
    nats = conf_dict['nats']['uri'] if 'nats' not in kwargs or kwargs['uri'] is None else kwargs['nats']
    debug = kwargs['debug']

    file = None
    level = "critical"
    if debug:
        file = conf_dict['log']['path'] + os.sep + cls.__name__.lower() + '.log'
        level = conf_dict['log']['level']

    log.setup_logger(file=file, level=level)
    logger = log.get_logger()
    logger.debug('初始化数据库')
    db = StockDB(uri=uri, pool=pool)
    if not db.init():
        print('初始化数据库失败')
        return

    if 'quot' in kwargs:
        fetch.init()

    kwargs['options'] = {'servers': [nats]}
    kwargs['db'] = db

    trader = cls(**kwargs)
    trader.start()


class TradeReal(StockNats):
    """
    account: {id, options}
    """

    def __init__(self, **kwargs):
        """

        :param kwargs:
            options, db, topic, parent_topic, data
        """
        self.loop = asyncio.get_event_loop()
        super().__init__(loop=self.loop, options=kwargs['options'])

        self.db = kwargs['db']

        self.topic_cmd = kwargs['topic']
        self.parent_topic = kwargs['parent_topic']
        self.data = kwargs['data']

        self.quot_topic_cmd = 'topic:bbq.quotation.command'

        self.quot_topic = None

        self.cmd_handlers = {
            'cancel': self.on_cancel
        }
        self.add_handler(self.topic_cmd, self.cmd_handlers)

    async def init(self):
        await self.nats_task()
        payload = dict(cmd='subscribe', data=dict(
            type='realtime',
            frequency='1min',
            start='2020-08-01 09:00:00',
            end='2020-08-01 15:00:00',
            index_list=['000001.SH', '399006.SZ'],
            stock_list=['000001.SZ']
        ))
        suc = await self.sub_quot(payload=payload)
        if not suc:
            await self.stop()
        return suc

    async def loop_stop(self):
        await asyncio.sleep(0.1, self.loop)
        self.loop.stop()

    async def on_cancel(self, data):
        await self.unsub_quot(payload=dict(cmd='unsubscribe', data=dict(subject=self.quot_topic)))
        self.loop.create_task(self.stop())
        self.loop.create_task(self.loop_stop())
        return 'OK'

    async def on_quot(self, data):
        self.log.info('on_quot: {}'.format(data))

    async def unsub_quot(self, payload: Dict):
        try:
            data = await self.request(subject=self.quot_topic_cmd, data=payload, timeout=15)

            if data['status'] != 'OK':
                self.log.error('取消订阅行情异常, 失败={}'.format(data['msg']))
                return False

            await self.unsubscribe(subject=self.quot_topic)
        except Exception as e:
            self.log.error('取消订阅行情异常, ex={}, callstack={}'.format(e, traceback.format_exc()))
            return False
        return True

    async def sub_quot(self, payload: Dict):
        try:
            data = await self.request(subject=self.quot_topic_cmd, data=payload, timeout=15)

            if data['status'] != 'OK':
                self.log.error('订阅行情异常, 失败={}'.format(data['msg']))
                return False

            subject = data['data']['subject']
            self.quot_topic = subject
            self.log.info('监听行情topic: {}'.format(subject))
            self.add_handler(subject, dict(quotation=self.on_quot))
            await self.subscribe(subject=subject)
        except Exception as e:
            self.log.error('订阅行情异常, ex={}, callstack={}'.format(e, traceback.format_exc()))
            return False
        return True

    def start(self):
        try:
            if self.loop.run_until_complete(self.init()):
                self.loop.create_task(self.subscribe(self.topic_cmd))
                self.loop.run_forever()
        except Exception as e:
            self.log.error(
                '{} start 异常, ex={}, callstack={}'.format(self.__class__.__name__, e, traceback.format_exc()))
        finally:
            self.loop.close()


class TradeBacktest(TradeReal):
    """
    strategy: {id, options},
    account: {id, options}
    risk: {id, options}
    code: {index, stock}
    """

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    async def init(self):
        await self.nats_task()
        self.log.info('TradeBacktest init data={}'.format(self.data))
        payload = dict(cmd='subscribe', data=dict(
            type='backtest',
            frequency='5min',
            start='2020-08-01 09:00:00',
            end='2020-08-01 15:00:00',
            index_list=['000001.SH', '399006.SZ'],
            stock_list=['000001.SZ']
        ))
        return await self.sub_quot(payload=payload)


class Trader(StockNats):
    def __init__(self, **kwargs):
        """

        :param kwargs:
            debug, options, db
        """
        self.loop = asyncio.get_event_loop()
        super().__init__(loop=self.loop, options=kwargs['options'])

        self.debug = kwargs['debug']
        self.db = kwargs['db']

        self.topic_cmd = 'topic:bbq.trader.command'

        self.cmd_handlers = {
            'backtest': self.on_backtest,
            'trade': self.on_trade,
            'cancel': self.on_cancel
        }
        self.add_handler(self.topic_cmd, self.cmd_handlers)

        self.running = False

        self.topics = {}

        # account = Account(db=self.db)
        #
        # risk = Risk(account=account)
        # broker = Broker(account=account)
        # strategy = Strategy(account=account)
        #
        # while True：
        #     check_risk
        #
        # # broker.run()
        #     while True:
        #         xx = check_event()
        #         xx->onEvent()
        #
        # while True:
        #     quot = xx;
        #     signl = risk->on_quot...
        #
        #     onSignalxx
        #
        #
        # account->on_quot

    async def on_backtest(self, data):
        self.log.info('backtest请求: {}'.format(data))
        topic = 'topic:bbq.trader.backtest.command.{}'.format(str(uuid.uuid4()).replace('-', ''))
        p = Process(target=run, kwargs=dict(cls=TradeBacktest,
                                            topic=topic,
                                            parent_topic=self.topic_cmd,
                                            uri=self.db.uri,
                                            pool=self.db.pool,
                                            debug=self.debug,
                                            nats=self.options['servers'][0],
                                            quot=True,
                                            data=data))
        p.start()
        self.topics[topic] = p
        self.log.info('backtest 进程启动, pid={}, topic={}'.format(p.pid, topic))
        return "OK", "SUCCESS", topic

    async def on_trade(self, data):
        self.log.info('trade请求: {}'.format(data))
        topic = 'topic:bbq.trader.trade.command.{}'.format(str(uuid.uuid4()).replace('-', ''))
        p = Process(target=run, kwargs=dict(cls=TradeReal,
                                            topic=topic,
                                            parent_topic=self.topic_cmd,
                                            uri=self.db.uri,
                                            pool=self.db.pool,
                                            debug=self.debug,
                                            nats=self.options['servers'][0],
                                            quot=False,
                                            data=data))
        p.start()
        self.topics[topic] = p
        self.log.info('trade 进程启动, pid={}, topic={}'.format(p.pid, topic))
        return "OK", "SUCCESS", topic

    async def on_cancel(self, data):
        pass

    async def proc_check_task(self):
        while self.running:
            await asyncio.sleep(5)
            if len(self.topics) <= 0:
                continue

            quit_proc = []
            for topic, proc in self.topics.items():
                if not proc.is_alive():
                    self.log.info('进程退出, 获取退出码...')
                    proc.join()
                    self.log.info('进程退出, topic: {}, pid: {}, exitcode: {}'.format(topic, proc.pid, proc.exitcode))
                    quit_proc.append(topic)
            for topic in quit_proc:
                del self.topics[topic]

    def start(self):
        try:
            if self.loop.run_until_complete(self.nats_task()):
                self.running = True
                self.loop.create_task(self.subscribe(self.topic_cmd))
                self.loop.create_task(self.proc_check_task())
                self.loop.run_forever()
        except Exception as e:
            self.log.error('{} start 异常, ex={}, callstack={}'.format(self.__name__, e, traceback.format_exc()))
        finally:
            self.loop.close()


@click.command()
@click.option('--uri', type=str, help='mongodb connection uri')
@click.option('--pool', default=0, type=int, help='mongodb connection pool size')
@click.option('--nats', type=str, help='mongodb uri')
@click.option('--debug/--no-debug', default=True, help='show debug log')
def main(uri: str, pool: int, nats: str, debug: bool):
    mp.set_start_method('spawn')
    run(uri=uri, pool=pool, nats=nats, debug=debug, cls=Trader)


if __name__ == '__main__':
    main()
