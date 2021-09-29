import bbq.log as log
from functools import wraps
from bbq.message import Message


def discard_push(func):
    @wraps(func)
    async def wrapper(self, *args, **kwargs):
        if self.trader.is_backtest() or not self.is_inited:
            return
        return await func(self, *args, **kwargs)

    return wrapper


class MsgPush(Message):
    def __init__(self, trader):
        super().__init__()
        self.log = log.get_logger(self.__class__.__name__)
        self.trader = trader

    @discard_push
    async def send_email(self, email, title, content):
        return await super(MsgPush, self).send_email(email=email, title=title, content=content)

    @discard_push
    async def push_wechat(self, title, content) -> bool:
        is_succ = await super(MsgPush, self).push_wechat(title=title, content=content)
        if not is_succ:
            self.log.error('push wechat failed')
        return is_succ

