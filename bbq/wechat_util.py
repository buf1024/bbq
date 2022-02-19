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

