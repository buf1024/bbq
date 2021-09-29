import aiohttp
import aiosmtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import json
from urllib.parse import quote


class Message:
    def __init__(self):
        self.msg_conf = dict(email=dict(host='smtp.126.com', port=465, email='', secret=''),
                             wechat=dict(url=''))

        self.is_inited = False

    def init(self, **opt) -> bool:
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
                return False
            wechat['url'] = 'https://sctapi.ftqq.com/{}.send'.format(token)

        for k, v in email.items():
            if len(str(v)) == 0:
                return False
        self.is_inited = True

        return True

    async def send_email(self, email, title, content):
        email_conf = self.msg_conf['email']
        message = MIMEMultipart('alternative')
        message['From'] = email_conf['email']
        message['To'] = email
        message['Subject'] = title

        html_message = MIMEText(content, 'html', 'utf-8')
        message.attach(html_message)
        return await aiosmtplib.send(message, hostname=email_conf['host'], port=email_conf['port'],
                                     username=email_conf['email'], password=email_conf['secret'], use_tls=True)

    async def push_wechat(self, title, content) -> bool:
        if not self.is_inited:
            return False

        data = 'text={}&desp={}'.format(quote(title), quote(content))
        url = '{}?{}'.format(self.msg_conf['wechat']['url'], data)
        async with aiohttp.ClientSession() as session:
            async with session.get(url=url) as req:
                text = await req.text()
                # {"errno": 0, "errmsg": "success", "dataset": "done"}
                try:
                    js = json.loads(text)
                    if js['errno'] != 0:
                        return False
                except Exception as e:
                    return False
                return True


if __name__ == '__main__':
    from bbq.common import run_until_complete
    from datetime import datetime

    msg = Message()
    msg.init(email=dict(email='barbarianquant@126.com', secret='DAZXJPKTVXRVOESQ'),
             wechat=dict(token='SCT79912TBBpSAKVv98IdQ3RWqCoQQNW2'))


    async def test_wechat():
        await msg.push_wechat('买入信号',
                              'sh601099(太平洋)\n\n**买入**信号(**神算子4号**策略触发)\n\nprice={} volume={}\n\n time={}'.format(
                                  4.02,
                                  5000,
                                  datetime.now()))


    async def test_email():
        rest = await msg.send_email('450171094@qq.com',
                                    '成交事件',
                                    '<h1>sh601099(<span>太平洋</span>)</h1><br>**买入**信号(<bold>神算子4号</bold>策略触发)<br>price={} volume={}<br><br>time={}'.format(
                                        4.02,
                                        5000,
                                        datetime.now())
                                    )

        print(rest)


    run_until_complete(
        # test_wechat()
        test_email()
    )
    print('done')
