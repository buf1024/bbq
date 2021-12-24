import json
from urllib.parse import quote
import aiohttp


class WechatUtil:
    """
    方糖微信推送，不过没有数字提示，不如qq邮箱微信提示醒目
    """
    def __init__(self, token: str):
        self.url = 'https://sctapi.ftqq.com/{}.send'.format(token)

    async def push_wechat(self, title, content) -> bool:
        data = 'text={}&desp={}'.format(quote(title), quote(content))
        url = '{}?{}'.format(self.url, data)
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

    msg = WechatUtil(token='SCT79912TBBpSAKVv98IdQ3RWqCoQQNW2')


    async def test_wechat():
        await msg.push_wechat('买入信号屁',
                              'sh601099(太平洋)\n\n**买入**信号(**神算子4号**策略触发)\n\nprice={} volume={}\n\n time={}'.format(
                                  4.02,
                                  5000,
                                  datetime.now()))


    run_until_complete(
        test_wechat()
    )
    print('done')
