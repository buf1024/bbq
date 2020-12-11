import aiohttp
import aiosmtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import json
from urllib.parse import quote

__msg_conf = {
    'email': {
        'host': 'smtp.126.com',
        'port': 465,
        'email': '',
        'secret': ''
    },
    'wechat': {
        'header': {
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/73.0.3683.86 Safari/537.36'
        },
        'url': ''
    }
}


def int_push_msg(opt) -> bool:
    email = __msg_conf['email']
    if 'email' in opt:
        email_opt = opt['email']
        email['host'] = email_opt['host'] if 'host' in email_opt else email['host']
        email['port'] = email_opt['port'] if 'port' in email_opt else email['port']
        email['email'] = email_opt['email'] if 'email' in email_opt else email['email']
        email['secret'] = email_opt['secret'] if 'secret' in email_opt else email['secret']

    wechat = __msg_conf['wechat']
    if 'wechat' in opt:
        wechat_opt = opt['wechat']
        secret = wechat_opt['secret'] if 'secret' in wechat_opt else ''
        if len(secret) == 0:
            print('wechat secret is null')
            return False

        wechat['url'] = 'https://sc.ftqq.com/{}.send'.format(secret)

    for k, v in email.items():
        if len(str(v)) == 0:
            print('email config key={} is null'.format(k))
            return False

    return True


async def send_email(email, subject, content):
    email_conf = __msg_conf['email']
    message = MIMEMultipart('alternative')
    message['From'] = email_conf['email']
    message['To'] = email
    message['Subject'] = subject

    html_message = MIMEText(content, 'html', 'utf-8')
    message.attach(html_message)
    return await aiosmtplib.send(message, hostname=email_conf['host'], port=email_conf['port'],
                                 username=email_conf['email'], password=email_conf['secret'], use_tls=True)


async def push_wechat(title, text) -> bool:
    data = 'text={}&desp={}'.format(quote(title), quote(text))
    url = '{}?{}'.format(__msg_conf['wechat']['url'], data)
    async with aiohttp.ClientSession() as session:
        async with session.get(url=url,
                               headers=__msg_conf['wechat']['header']) as req:
            text = await req.text()
            # {"errno": 0, "errmsg": "success", "dataset": "done"}
            try:
                js = json.loads(text)
                if js['errno'] != 0:
                    print('send failed')
                    return False
            except Exception as e:
                print('send failed, e={}'.format(e))
                return False
            return True


if __name__ == '__main__':
    from bbq.common import run_until_complete
    from datetime import datetime

    int_push_msg(opt=dict(email=dict(email='barbarianquant@126.com', secret='DAZXJPKTVXRVOESQ'),
                          wechat=dict(secret='SCU65265Tc40310137ed88be0281163cbc854ff925db79a6189f4d')))


    async def test_wechat():
        await push_wechat('买入信号',
                          'sh601099(太平洋)\n\n**买入**信号(**神算子4号**策略触发)\n\nprice={} volume={}\n\n time={}'.format(4.02,
                                                                                                              5000,
                                                                                                              datetime.now()))


    async def test_email():
        rest = await send_email('barbarianquant@126.com',
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
