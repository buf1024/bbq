import bbq.log as log
from functools import wraps
from bbq.wechat_util import WechatUtil
from bbq.email_util import EmailUtil


def discard_push(func):
    @wraps(func)
    async def wrapper(self, *args, **kwargs):
        if self.trader.is_backtest() or not self.is_inited:
            return
        return await func(self, *args, **kwargs)

    return wrapper


class MsgPush:
    def __init__(self, trader):
        super().__init__()
        self.log = log.get_logger(self.__class__.__name__)
        self.trader = trader
        self.email_util = None
        self.wechat_util = None
        self.is_inited = False
        self.notify = None
        self.prefix = None
        if 'message' in self.trader.config:
            conf = self.trader.config['message']['email']
            user, passwd = conf['user'], conf['secret']
            smtp_host, smtp_port = conf['smtp-host'], conf['smtp-port']
            if 'notify' in conf:
                self.notify = conf['notify']
            if 'prefix' in conf:
                self.prefix = conf['prefix']

            self.email_util = EmailUtil(user=user, passwd=passwd, smtp_host=smtp_host, smtp_port=smtp_port)

            conf = self.trader.config['message']['wechat']
            token = conf['token']
            self.wechat_util = WechatUtil(token=token)

            self.is_inited = True

    @discard_push
    async def send_email(self, email, title, content):
        if self.email_util is not None:
            return await self.email_util.send_email(email=email, title=title, content=content)

    @discard_push
    async def push_wechat(self, title, content) -> bool:
        if self.wechat_util is not None:
            is_succ = await self.wechat_util.push_wechat(title=title, content=content)
            if not is_succ:
                self.log.error('push wechat failed')
            return is_succ
        return False

    async def push(self, title, content):
        if self.notify is not None:
            if self.prefix is not None:
                title = '{}#{}'.format(self.prefix, title)
            return await self.send_email(email=self.notify, title=title, content=content)

        return await self.push_wechat(title=title, content=content)
