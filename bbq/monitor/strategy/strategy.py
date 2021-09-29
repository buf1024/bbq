import bbq.log as log
import asyncio


class Strategy:
    def __init__(self, db_stock, db_fund, notify):
        """

        :param db_stock
        """
        self.log = log.get_logger(self.__class__.__name__)
        self.db_stock = db_stock
        self.db_fund = db_fund
        self.notify = notify

        self.is_running = False

    def desc(self):
        pass

    async def init(self, **kwargs):
        """
        初始化接口
        :param kwargs:
        :return: True/False
        """
        return True

    async def stop(self):
        self.is_running = False
        return True

    async def monitor(self):
        pass

    async def start(self):
        self.is_running = True
        while self.is_running:
            await self.monitor()
            await asyncio.sleep(1)

    async def notify_email(self, email, title, content):
        await self.notify(evt='evt_notify',
                          payload=dict(type='email',
                                       content=dict(email=email, title=title, content=content)))

    async def notify_wechat(self, title, content):
        await self.notify(evt='evt_notify',
                          payload=dict(type='wechat',
                                       content=dict(title=title, content=content)))
