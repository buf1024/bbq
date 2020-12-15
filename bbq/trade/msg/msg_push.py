import aiohttp
import aiosmtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import json
from urllib.parse import quote
import bbq.log as log
from functools import wraps


def discard_push(func):
    @wraps(func)
    async def wrapper(self, *args, **kwargs):
        if self.trader.is_backtest() or not self.is_inited:
            return
        return await func(self, *args, **kwargs)

    return wrapper


class MsgPush:
    def __init__(self):
        self.log = log.get_logger(self.__class__.__name__)
        self.msg_conf = dict(email=dict(host='smtp.126.com', port=465, email='', secret=''),
                             wechat=dict(url=''))

        self.is_inited = False
        self.trader = None

    def init_push(self, trader, opt) -> bool:
        email = self.msg_conf['email']
        if 'email' in opt:
            email_opt = opt['email']
            email['host'] = email_opt['host'] if 'host' in email_opt else email['host']
            email['port'] = email_opt['port'] if 'port' in email_opt else email['port']
            email['email'] = email_opt['email'] if 'email' in email_opt else email['email']
            email['secret'] = email_opt['secret'] if 'secret' in email_opt else email['secret']

        wechat = self.msg_conf['wechat']
        if 'wechat' in opt:
            wechat_opt = opt['wechat']
            token = wechat_opt['token'] if 'token' in wechat_opt else ''
            if len(token) == 0:
                self.log.error('wechat token is null')
                return False

            wechat['url'] = 'https://sc.ftqq.com/{}.send'.format(token)

        for k, v in email.items():
            if len(str(v)) == 0:
                self.log.error('email config key={} is null'.format(k))
                return False

        self.trader = trader
        self.is_inited = True
        return True

    @discard_push
    async def send_email(self, email, subject, content):
        email_conf = self.msg_conf['email']
        message = MIMEMultipart('alternative')
        message['From'] = email_conf['email']
        message['To'] = email
        message['Subject'] = subject

        html_message = MIMEText(content, 'html', 'utf-8')
        message.attach(html_message)
        return await aiosmtplib.send(message, hostname=email_conf['host'], port=email_conf['port'],
                                     username=email_conf['email'], password=email_conf['secret'], use_tls=True)

    @discard_push
    async def wechat_push(self, title, text) -> bool:
        if not self.is_inited:
            self.log.warn('wechat_push not inited')
            return False

        data = 'text={}&desp={}'.format(quote(title), quote(text))
        url = '{}?{}'.format(self.msg_conf['wechat']['url'], data)
        async with aiohttp.ClientSession() as session:
            async with session.get(url=url) as req:
                text = await req.text()
                # {"errno": 0, "errmsg": "success", "dataset": "done"}
                try:
                    js = json.loads(text)
                    if js['errno'] != 0:
                        self.log.error('wechat push failed: {}'.format(js))
                        return False
                except Exception as e:
                    self.log.error('send failed, e={}'.format(e))
                    return False
                return True


if __name__ == '__main__':
    from bbq.common import run_until_complete
    from datetime import datetime

    msg_push = MsgPush()
    msg_push.init_push(opt=dict(email=dict(email='barbarianquant@126.com', secret='DAZXJPKTVXRVOESQ'),
                                wechat=dict(token='SCU65265Tc40310137ed88be0281163cbc854ff925db79a6189f4d')))


    async def test_wechat():
        await msg_push.wechat_push('买入信号',
                                   'sh601099(太平洋)\n\n**买入**信号(**神算子4号**策略触发)\n\nprice={} volume={}\n\n time={}'.format(
                                       4.02,
                                       5000,
                                       datetime.now()))


    async def test_email():
        rest = await msg_push.send_email('barbarianquant@126.com',
                                         '成交事件',
                                         '<h1>sh601099(<span>太平洋</span>)</h1><br>**买入**信号(<bold>神算子4号</bold>策略触发)<br>price={} volume={}<br><br>time={}'.format(
                                             4.02,
                                             5000,
                                             datetime.now())
                                         )

        print(rest)


    run_until_complete(
        test_wechat()
        # test_email()
    )
    print('done')
