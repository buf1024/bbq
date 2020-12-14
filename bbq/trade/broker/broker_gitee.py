from .broker import Broker
from bbq.trade.account import Account
from bbq.trade.msg.msg_gitee import MsgGitee
from typing import Dict
import asyncio
import copy


class BrokerGitee(Broker):
    def __init__(self, broker_id, account: Account):
        super().__init__(broker_id=broker_id, account=account)
        self.gitee = None
        self.timeout = 3

    async def init(self, opt: Dict) -> bool:
        if 'token' not in opt:
            self.log.error('miss gitee token')
            return False
        self.gitee = MsgGitee()
        if not self.gitee.init_gitee(token=opt['token']):
            self.log.error('init gitee failed.')
            return False

        await self.trader.task_queue.put('gitee_poll_task')
        self.trader.loop.create_task(self.gitee_poll_task())

        return True

    async def on_entrust(self, evt, payload):
        pass

    async def gitee_poll_task(self):
        await self.trader.task_queue.get()
        while self.trader.running:
            # todo check
            await asyncio.sleep(self.timeout)

        self.trader.task_queue.task_done()
