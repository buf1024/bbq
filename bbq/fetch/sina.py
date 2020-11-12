from urllib.request import Request, urlopen
import aiohttp
import re
import datetime
from typing import Any, Dict


class Sina:
    regex = re.compile(r'(\w+)=([^\s][^,]+?)%s%s' % (r',([\.\d]+)' * 29, r',([-\.\d:]+)' * 2))
    url = 'http://hq.sinajs.cn/?format=text&list='

    async def _get_quot(self, code_list) -> Dict[str, Dict[str, Any]]:
        url = self.url + code_list

        async with aiohttp.request('get', url, headers={
            'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_6) AppleWebKit/537.36 (KHTML, '
                          'like Gecko) Chrome/73.0.3683.86 Safari/537.36'
        }) as req:
            resp = await req.text()
            group_objs = self.regex.finditer(resp)
            quot_dict = {}
            for match_obj in group_objs:
                quots = match_obj.groups()
                code = quots[0][2:] + '.' + quots[0][:2].upper()
                quot_dict[code] = dict(
                    code=code,
                    name=quots[1],
                    open=float(quots[2]), pre_close=float(quots[3]),
                    now=float(quots[4]), high=float(quots[5]), low=float(quots[6]),
                    buy=float(quots[7]), sell=float(quots[8]),
                    vol=int(quots[9]), amount=float(quots[10]),
                    bid=[(int(quots[11]), float(quots[12])), (int(quots[13]), float(quots[14])),
                         (int(quots[15]), float(quots[16])), (int(quots[17]), float(quots[18])),
                         (int(quots[19]), float(quots[20]))],
                    ask=[(int(quots[21]), float(quots[22])), (int(quots[23]), float(quots[24])),
                         (int(quots[25]), float(quots[26])), (int(quots[27]), float(quots[28])),
                         (int(quots[29]), float(quots[30]))],
                    date=datetime.datetime.strptime(quots[31], '%Y-%m-%d'),
                    datetime=datetime.datetime.strptime(quots[31] + ' ' + quots[32], '%Y-%m-%d %H:%M:%S'),
                )
            return quot_dict

    def _get_quot_sync(self, code_list) -> Dict[str, Dict[str, Any]]:
        url = self.url + code_list

        req = Request(method='get', url=url, headers={
            'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_6) AppleWebKit/537.36 (KHTML, '
                          'like Gecko) Chrome/73.0.3683.86 Safari/537.36'})
        resp = urlopen(req).read().decode('gbk')
        group_objs = self.regex.finditer(resp)
        quot_dict = {}
        for match_obj in group_objs:
            quots = match_obj.groups()
            code = quots[0][2:] + '.' + quots[0][:2].upper()
            quot_dict[code] = dict(
                code=code,
                name=quots[1],
                open=float(quots[2]), pre_close=float(quots[3]),
                now=float(quots[4]), high=float(quots[5]), low=float(quots[6]),
                buy=float(quots[7]), sell=float(quots[8]),
                vol=int(quots[9]), amount=float(quots[10]),
                bid=[(int(quots[11]), float(quots[12])), (int(quots[13]), float(quots[14])),
                     (int(quots[15]), float(quots[16])), (int(quots[17]), float(quots[18])),
                     (int(quots[19]), float(quots[20]))],
                ask=[(int(quots[21]), float(quots[22])), (int(quots[23]), float(quots[24])),
                     (int(quots[25]), float(quots[26])), (int(quots[27]), float(quots[28])),
                     (int(quots[29]), float(quots[30]))],
                date=datetime.datetime.strptime(quots[31], '%Y-%m-%d'),
                datetime=datetime.datetime.strptime(quots[31] + ' ' + quots[32], '%Y-%m-%d %H:%M:%S'),
            )
        return quot_dict

    async def get_rt_quot(self, codes):
        if codes is None:
            return None
        params = []
        for code in codes:
            p = code.lower().split('.')
            params.append(p[1] + p[0])
        code_list = ','.join(params)

        return await self._get_quot(code_list)

    def get_rt_quot_sync(self, codes):
        if codes is None:
            return None
        params = []
        for code in codes:
            p = code.lower().split('.')
            params.append(p[1] + p[0])
        code_list = ','.join(params)

        # try:
        #     evt_loop = asyncio.get_event_loop()
        # except RuntimeError:
        #     evt_loop = asyncio.new_event_loop()
        #     asyncio.set_event_loop(evt_loop)
        #
        # tasks = [self._get_quot(code_list)]
        # quots = evt_loop.run_until_complete(asyncio.gather(*tasks))
        # return quots[0]

        return self._get_quot_sync(code_list)


if __name__ == '__main__':
    q = Sina()
    quot = q.get_rt_quot_sync(codes=['000001.SZ', '601099.SH'])
    print(quot)
