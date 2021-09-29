from .strategy import Strategy
from datetime import datetime, timedelta
import random


class Dummy(Strategy):
    def __init__(self, db_stock, db_fund, notify):
        super().__init__(db_stock, db_fund, notify)

        self.last_notify = datetime.now()
        self.next_interval = random.randint(1, 15)
        self.type = ['email', 'wechat']

    async def monitor(self):
        now = datetime.now()
        delta = now - self.last_notify
        self.log.debug('delta: {}, next: {}'.format(delta, self.next_interval))
        if delta.seconds > self.next_interval:
            typ = random.choice(self.type)
            if typ == 'wechat':
                await self.notify_wechat('测试监控', '测试内容')
            if typ == 'email':
                await self.notify_email('450171094@qq.com', '测试监控', '测试内容')

            self.last_notify = now
            self.next_interval = random.randint(1, 15)
