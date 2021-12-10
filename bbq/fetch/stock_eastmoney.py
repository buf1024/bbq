from datetime import datetime
import requests
import pandas as pd
from typing import Optional
import random
import json


class BaseRequest:
    def __init__(self):
        # request header
        self.user_agent = "Mozilla/5.0 (Windows NT 6.1; WOW64) " \
                          "AppleWebKit/537.36 (KHTML, like Gecko) " \
                          "Chrome/57.0.2987.133 Safari/537.36 "

        # simulate http request
        self.session = requests.Session()
        self.session.headers['User-Agent'] = self.user_agent

    def add_headers(self, headers):
        self.session.headers.update(headers)

    @property
    def cookies(self):
        return self.session.cookies

    def do_request(self, url, param=None, method="GET", typ="text", encoding=None, json=None, **kwargs):

        if method == "GET":
            res = self.session.get(url, params=param, **kwargs)
        else:
            if json is not None:
                res = self.session.post(url, json=json, **kwargs)
            else:
                res = self.session.post(url, data=param, **kwargs)

        if res.status_code != 200:
            return None

        if typ == 'text':
            return res.text
        else:
            return res.content

    def prepare_cookies(self, url):
        response = self.do_request(url, None)

        if response is not None:
            return self.cookies
        else:
            return None


class StockEastmoney(BaseRequest):
    def __init__(self):
        super().__init__()

    def get_stock_margin(self, code: str, start: datetime = None, end: datetime = None) -> Optional[pd.DataFrame]:
        """
        股票代码(CODE) 股票名称(NAME)
        交易日期(DATE)	收盘价(元)(SPJ) 涨跌幅(%)(ZDF)
        融资: 余额(元)(RZYE)	余额占流通市值比(%)(RZYEZB)	买入额(元)(RZMRE)	偿还额(元)(RZCHE)	净买入(元)(RZJME)
        融券: 余额(元)(RQYE)	余量(股)(RQYL)	卖出量(股)(RQMCL)	偿还量(股)(RQCHL)	净卖出(股)(RQJMG)
        融资融券余额(元)(RZRQYE)	融资融券余额差值(元)(RZRQYECZ)
        """
        org_code = code
        if code.startswith('sh') or code.startswith('sz'):
            code = code[2:]

        def get_url(p, ps):
            table = 'datatable' + ''.join([str(random.randint(0, 9)) for _ in range(7)])
            url = r'https://datacenter-web.eastmoney.com/api/data/get?callback={table}&type=RPTA_WEB_RZRQ_GGMX&sty=ALL&source=WEB&st=date&sr=-1&p={page}&ps={page_size}&filter=(scode="{code}")&pageNo={page}&pageNum={page}&pageNumber={page}&_={tm}'
            if p == 1:
                url = r'https://datacenter-web.eastmoney.com/api/data/get?callback={table}&type=RPTA_WEB_RZRQ_GGMX&sty=ALL&source=WEB&st=date&sr=-1&p={page}&ps={page_size}&filter=(scode="{code}")&_={tm}'
            url = url.format(table=table, code=code, page_size=ps, page=p,
                             tm=str(datetime.now().timestamp())[:-3].replace('.', ''))
            return url, table

        pre_url = r'https://data.eastmoney.com/rzrq/detail/{}.html'.format(code)
        cookies = self.prepare_cookies(pre_url)

        df = pd.DataFrame()
        page_size = 50
        page = 1
        while True:
            req_url, tab = get_url(p=page, ps=page_size)
            data = self.do_request(url=req_url, cookies=cookies)
            if data is None:
                break
            data = data[len(tab) + 1:-2]
            data = json.loads(data)
            if data['code'] != 0:
                break

            df_tmp = pd.DataFrame(data['result']['data'])
            df_tmp['CODE'] = org_code
            df_tmp['NAME'] = df_tmp['SECNAME']
            df_tmp['DATE'] = pd.to_datetime(df_tmp['DATE'], format='%Y-%m-%d %H:%M:%S')
            if not df_tmp.empty:
                df = pd.concat((df, df_tmp))
            if start is not None:
                m = df['DATE'].min()
                if start >= m:
                    break

            pages = data['result']['pages']
            if page >= pages:
                break
            page = page + 1

        if start is not None and not df.empty:
            df = df[df['DATE'] >= start]

        if end is not None and not df.empty:
            df = df[df['DATE'] <= end]

        if not df.empty:
            df = df[['CODE', 'NAME',
                     'DATE', 'SPJ', 'ZDF',
                     'RZYE', 'RZYEZB', 'RZMRE', 'RZCHE', 'RZJME',
                     'RQYE', 'RQYL', 'RQMCL', 'RQCHL', 'RQJMG',
                     'RZRQYE', 'RZRQYECZ']]
            df.rename(columns={'CODE': 'code', 'NAME': 'name',
                               'DATE': 'trade_date', 'SPJ': 'spj', 'ZDF': 'zdf',
                               'RZYE': 'rzye', 'RZYEZB': 'rzyezb', 'RZMRE': 'rzmre', 'RZCHE': 'rzche', 'RZJME': 'rzjme',
                               'RQYE': 'rqye', 'RQYL': 'rqyl', 'RQMCL': 'rqmcl', 'RQCHL': 'rqchl', 'RQJMG': 'rqjmg',
                               'RZRQYE': 'rzrqye', 'RZRQYECZ': 'rzrqyecz'}, inplace=True)

        return df.reset_index(drop=True)


if __name__ == '__main__':
    s = StockEastmoney()
    tdf = s.get_stock_margin('sh600099', start=datetime(year=2021, month=12, day=1))
    print(tdf.columns)
