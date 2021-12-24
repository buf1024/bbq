import asyncio
import traceback
import bbq.log as log
from bbq.data.mongodb import MongoDB
from typing import Dict
from bbq.wechat_util import WechatUtil
from bbq.monitor.strategy import init_strategy, get_strategy


class Monitor:
    def __init__(self, db_stock: MongoDB, db_fund: MongoDB, config: Dict):
        self.log = log.get_logger(self.__class__.__name__)
        self.loop = asyncio.get_event_loop()
        self.db_stock = db_stock
        self.db_fund = db_fund
        self.config = config

        self.notify_queue = asyncio.Queue()
        self.task_queue = asyncio.Queue()
        self.strategy_insts = []

        self.is_running = False

        self.msg_push = Message()

    def signal_handler(self, signum, frame):
        print('catch signal: {}, stop trade...'.format(signum))
        self.is_running = False
        self.notify_queue.put_nowait(('__evt_term', None))

    async def start(self):
        init_strategy(self.config['monitor']['strategy-path'])
        strategies_conf = self.config['monitor']['strategy']
        for strategy_conf in strategies_conf:
            name = strategy_conf['id']
            opt = strategy_conf['option']
            cls = get_strategy(name)
            if cls is None:
                self.log.error('monitor strategy {} not found'.format(name))
                return False
            inst = cls(db_stock=self.db_stock, db_fund=self.db_fund, notify=self.notify)
            if not await inst.init(**opt if opt is not None else {}):
                self.log.error('init strategy {} failed, opt={}'.format(name, opt))
                return False
            self.strategy_insts.append(inst)

        if len(self.strategy_insts) == 0:
            self.log.info('no strategy to monitor')
            return False

        if 'message' in self.config:
            is_init = self.msg_push.init(**self.config['message'])
            if not is_init:
                self.log.error('初始化推送信息异常')
                return False
            self.log.info('push message inited')

        self.is_running = True
        for inst in self.strategy_insts:
            await self.task_queue.put('strategy_task')
            self.loop.create_task(self.strategy_task(inst))

        await self.task_queue.put('notify_task')
        self.loop.create_task(self.notify_task())

        await self.task_queue.join()
        return True

    async def notify(self, evt, payload):
        """

        :param evt:
        :param payload: {type: email/wechat, content: ...}
        :return:
        """
        await self.notify_queue.put((evt, payload))

    async def strategy_task(self, strategy):
        await self.task_queue.get()
        try:
            await strategy.start()
        except Exception as e:
            self.log.error('strategy_task ex: {}, callstack: {}'.format(e, traceback.format_exc()))
        finally:
            self.task_queue.task_done()

    async def notify_task(self):
        await self.task_queue.get()
        while self.is_running:
            evt, payload = await self.notify_queue.get()
            if evt == '__evt_term':
                for inst in self.strategy_insts:
                    await inst.stop()
                continue
            self.log.info('recv push: {}'.format(payload))
            try:
                typ, content = payload['type'], payload['content']
                push_func = self.msg_push.send_email if typ == 'email' else self.msg_push.push_wechat
                await push_func(**content)
            except Exception as e:
                self.log.error('notify_task ex: {}, callstack: {}'.format(e, traceback.format_exc()))
                continue
        self.task_queue.task_done()
