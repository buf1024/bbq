import re
import asyncio
import aiohttp
import pandas as pd
from datetime import datetime
import traceback
from collections import OrderedDict
from aiohttp import ClientConnectorError, ClientOSError
import json
from bbq import log
import random
from bbq.common import singleton


@singleton
class FundEastmoney:
    def __init__(self):
        self.log = log.get_logger(FundEastmoney.__name__)

        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/73.0.3683.86 Safari/537.36'
        }

        self.fund_plate_url = 'http://fund.eastmoney.com/api/FundTopicInterface.ashx'

        self.fund_list_url = 'http://fund.eastmoney.com/js/fundcode_search.js'
        self.fund_list_re = re.compile(r'\[(".*?")\]')
        self.fund_list_detail_re = re.compile(r'"(.*?)"')

        self.fund_net_url = 'http://fund.eastmoney.com/f10/F10DataApi.aspx?type=lsjz&code='
        self.fund_net_row_re = re.compile(r'<tr><td.*?>.*?</td></tr')
        self.fund_net_detail_re = re.compile(r'<td.*?>(.*?)</td>')
        self.fund_net_page_re = re.compile(r'records:(\d.*),pages:(\d.*),curpage:(\d.*)}')

        self.fund_info_url = 'http://fundf10.eastmoney.com/jbgk_{}.html'
        self.fund_info_rise_url = 'http://fund.eastmoney.com/{}.html'
        self.fund_info_html_re = re.compile(r'<table class="info w790">(.*?)</table>', re.M | re.DOTALL)
        self.fund_info_detail_re = re.compile(r'<th>(.*?)</th>*<t.*?>(.*?)</td>')

    async def retry_request(self, url):
        ex = None
        for i in range(5):
            try:
                async with aiohttp.ClientSession() as session:
                    for _ in range(15):
                        async with session.get(url=url, headers=self.headers) as req:
                            text = await req.text()
                            redirect = re.findall(r'location\.href.*?\"(.*?)";', text)
                            if len(redirect) == 0:
                                return text
                            url = redirect[0]
            except ClientConnectorError as e:
                ex = e
                self.log.error('连接错误, url={}, 第{}次重试'.format(url, i + 1))
                continue
            except ClientOSError as e:
                ex = e
                self.log.error('连接错误, url={}, 第{}次重试'.format(url, i + 1))
                continue
        raise ex

    async def get_block_list(self, fields=None, sync_fund=False):
        """
        板块列表
        :param sync_fund:
        :param fields: 过滤字段: 板块类别(plate) 板块名称(class) 日涨幅(day) 周涨幅(week) 月涨幅(month) 3月涨幅(3month)
                                6月涨幅(6month) 年涨幅(year) 跟新时间(date)

        :return DataFrame
        """
        tt = {'1': '综合指数', '2': '行业', '3': '概念', '4': '地区', '5': '含QDII的主题'}
        sort = {'SYL_D': 'day', 'SYL_Z': 'week', 'SYL_Y': 'month', 'SYL_3Y': '3month', 'SYL_6Y': '6month',
                'SYL_N': 'year'}
        df_list = []
        for tt_k, tt_v in tt.items():
            d = OrderedDict()
            for sort_k, sort_v in sort.items():
                url = '{}?callbackname=fundData&sort={}&sorttype=desc&pageindex=1&pagesize=500&dt=11&tt={}&rs=WRANK'.format(
                    self.fund_plate_url, sort_k, tt_k)
                js = await self.retry_request(url)
                js = js.replace('var fundData=', '')
                js = json.loads(js)
                for data in js['Datas']:
                    res = data.split(',')
                    if len(res) != 3:
                        continue
                    tid, name, rise = res[0], res[1], res[2]
                    if tid not in d:
                        d[tid] = OrderedDict()
                    info_d = d[tid]
                    info_d['plate'] = tt_v
                    info_d['name'] = name
                    info_d[sort_v] = rise
                    info_d['date'] = datetime.strptime(datetime.now().strftime('%Y%m%d'), '%Y%m%d')
            if sync_fund:
                for tp in d.keys():
                    self.log.debug('获取tp: {} 基金明细'.format(d[tp]['name']))
                    pageIndex = 1
                    while True:
                        url = "{}?callbackname=fundData&sort=NAVCHGRT&sorttype=asc&ft=&pageindex={}&pagesize=10&dt=10&tp={}&isbuy=1&v={}".format(
                            self.fund_plate_url, pageIndex, tp, random.random())
                        # self.log.debug('request: {}'.format(url))
                        js = await self.retry_request(url)
                        js = js.replace('var fundData=', '')
                        js = json.loads(js)
                        for data in js['Datas']:
                            code = data['FCODE']
                            if 'codes' not in d[tp]:
                                d[tp]['codes'] = [code]
                            else:
                                d[tp]['codes'].append(code)
                        if js['PageIndex'] + 1 != js['Pages']:
                            pageIndex += 1
                        else:
                            break
                    d[tp]['codes'] = ','.join(d[tp]['codes'])

            df_list = df_list + list(d.values())

        frame = pd.DataFrame(df_list)
        if fields is not None:
            frame = frame[[x.strip() for x in fields.split(',')]]
        return frame

    async def get_fund_list(self, fields=None):
        """
        基金列表
        :param fields: 过滤字段: 代码(code) 名称代码(name_code) 名称(name) 类型(type)

        :return DataFrame
        """
        resp = await self.retry_request(self.fund_list_url)

        group_objs = self.fund_list_re.finditer(resp)
        fund_list = []
        for match_obj in group_objs:
            detail = match_obj.groups()
            if len(detail) > 0:
                details = self.fund_list_detail_re.findall(detail[0])
                # 格式：["000001", "HXCZ", "华夏成长", "混合型", "HUAXIACHENGZHANG"]
                if len(details) == 5:
                    fund_list.append(dict(code=details[0],
                                          name_code=details[1],
                                          name=details[2],
                                          type=details[3]))
                    # name_pinying=details[4]))
        if len(fund_list) == 0:
            return None

        frame = pd.DataFrame(fund_list)
        if fields is not None:
            frame = frame[[x.strip() for x in fields.split(',')]]
        return frame

    async def get_fund_net(self, code, start=None, end=None, fields=None):
        """
        基金净值
        :param code: 基金代码
        :param start: 开始时间 datetime
        :param end: 结束时间 datetime
        :param fields: 过滤字段: 代码(code) 净值日期(date) 单位净值(net) 累计净值(net_accumulate) 日增长率(day_grow_rate)
                       申购状态(apply_status) 赎回状态(redeem_status) 分红送配(dividend)

        :return DataFrame
        """
        records, total_page, page, per = 0, 0, 1, 20

        def build_url():
            url = '{url}{code}&page={page}&per={per}'.format(url=self.fund_net_url, code=code, page=page, per=per)
            if start is not None:
                url = url + '&sdate=' + start.strftime('%Y-%m-%d')
            if end is not None:
                url = url + '&edate=' + end.strftime('%Y-%m-%d')

            return url

        net_list = []
        while True:
            req_url = build_url()
            # print('url=' + req_url)
            resp = await self.retry_request(req_url)
            rows = self.fund_net_row_re.findall(resp)
            for row in rows:
                # 净值日期	单位净值	累计净值	日增长率	申购状态	赎回状态	分红送配
                details = self.fund_net_detail_re.findall(row)
                if len(details) != 7:
                    break

                net_list.append(dict(code=code,
                                     date=datetime.strptime(details[0], '%Y-%m-%d'),
                                     net=0.0 if len(details[1].strip()) == 0 else float(details[1]),
                                     net_accumulate=0.0 if len(details[2].strip()) == 0 else float(details[2]),
                                     day_grow_rate=float(details[3][:-1]) if details[3].find('%') > 0 else 0.0,
                                     apply_status=details[4],
                                     redeem_status=details[5],
                                     dividend=details[6]
                                     ))

            page_details = self.fund_net_page_re.findall(resp)
            if len(page_details) != 1:
                break
            page_details = page_details[0]
            records, total_page, page = int(page_details[0]), int(page_details[1]), int(page_details[2])
            if len(net_list) >= records:
                break
            page += 1

        if len(net_list) == 0:
            return None

        frame = pd.DataFrame(net_list)
        if fields is not None:
            frame = frame[[x.strip() for x in fields.split(',')]]
        return frame

    async def get_fund_info(self, code, fields=None):
        """
        基金基本信息
        :param code: 基金代码
        :param fields: 过滤字段: 基金全称(full_name) 基金简称(short_name) 基金代码(code) 基金类型(type)
                               发行日期(issue_date) 成立日期(found_date) 规模(found_scale) 资产规模(scale)
                               资产规模时间(scale_date) 份额规模(share) 份额规模时间(share_date) 基金管理人(company)
                               基金托管人(bank) 基金经理人(manager) 成立来分红(dividend) 成立来分红次数(dividend_count)
        :return DataFrame
        """

        def transform(d, k, v):
            func_dict = {
                '基金全称': lambda x, y: ('full_name', y.strip()),
                '基金简称': lambda x, y: ('short_name', y.strip()),
                '基金代码': lambda x, y: ('code', y.replace('（主代码）', '').replace('（前端）', '').strip()),
                '基金类型': lambda x, y: ('type', y.strip()),
                '发行日期': lambda x, y: ('issue_date',
                                      datetime.strptime('19000101', '%Y%m%d') if len(y) == 0 else datetime.strptime(y,
                                                                                                                    '%Y年%m月%d日')),
                '成立日期': lambda x, y: ('found_date',
                                      datetime.strptime('19000101', '%Y%m%d') if len(y) == 0 else datetime.strptime(y,
                                                                                                                    '%Y年%m月%d日')),
                '规模': lambda x, y: (
                    'found_scale', 0 if y.find('-') >= 0 else float(re.findall(r'.*?(\d+\.\d+).*?', y)[0])),
                '资产规模': lambda x, y: ('scale', float(str(y).strip())),
                '资产规模时间': lambda x, y: ('scale_date', datetime.strptime(y.strip(), '%Y年%m月%d日')),
                '份额规模': lambda x, y: ('share', float(str(y).strip())),
                '份额规模时间': lambda x, y: ('share_date', datetime.strptime(y.strip(), '%Y年%m月%d日')),
                '基金管理人': lambda x, y: ('company', y.strip()),
                '基金托管人': lambda x, y: ('bank', y.strip()),
                '基金经理人': lambda x, y: ('manager', y.strip()),
                '成立来分红': lambda x, y: ('dividend', float(y.strip())),
                '成立来分红次数': lambda x, y: ('dividend_count', float(y.strip()))
            }
            # print('k={}, v={}'.format(k, v))
            if k in func_dict:
                try:
                    k, v = func_dict[k](k, v)
                    # print('tk={}, tv={}'.format(k, v))
                    d[k] = v
                except Exception as e:
                    self.log.error('转换异常: k={}, v={}, ex={}, stack={}'.format(k, v, e, traceback.format_exc()))

        def add_info(d, k, v):
            regex = [r'<a.*?>(.*?)</a>(.*)', r'<spa.*?>(.*?)</span>.*?<span>(.*?)<spa.*?>(.*?)</span>']
            if k == '基金经理人':
                v = ','.join(re.findall(r'<a.*?>(.*?)</a>', v))
            else:
                for reg in regex:
                    links = re.findall(reg, v)
                    if len(links) > 0:
                        v = ''
                        for link in links[0]:
                            v = v + link + ' '

            if k == '成立日期/规模':
                k1, k2 = [x.strip() for x in k.split('/')]
                v1, v2 = [x.strip() for x in v.split('/')]

                transform(d, k1, v1)
                transform(d, k2, v2)

                return

            if k == '资产规模' or k == '份额规模':
                try:
                    if v.find('-') >= 0:
                        v1, v2 = 0, '1900年01月01日'
                    else:
                        pattern = r'(\d+\.\d+).*?(\d+年\d+月\d+日).*'
                        if v.find('.') == -1:
                            pattern = r'(\d+).*?(\d+年\d+月\d+日).*'
                        v1, v2 = re.findall(pattern, v)[0]
                except Exception as e:
                    self.log.error('转换异常: k={}, v={}, ex={}, stack={}'.format(k, v, e, traceback.format_exc()))
                    return

                transform(d, k, v1)
                transform(d, k + '时间', v2)
                return

            if k == '成立来分红':
                try:
                    v1, v2 = re.findall(r'.*?(\d+\.\d+).*?(\d+).*', v)[0]
                except Exception as e:
                    self.log.error('转换异常: k={}, v={}, ex={}, stack={}'.format(k, v, e, traceback.format_exc()))
                    return

                transform(d, k, v1)
                transform(d, k + '次数', v2)

                return

            transform(d, k, v)

        url = self.fund_info_url.format(code)
        resp = await self.retry_request(url)
        info = self.fund_info_html_re.findall(resp)
        if len(info) <= 0:
            return None

        info = info[0]
        details = self.fund_info_detail_re.findall(info)
        if len(details) <= 0:
            return None
        info_dict = {}
        for detail in details:
            if len(detail) != 2:
                continue
            key, value = detail[0], detail[1]
            idx = value.find('<th>')
            if idx >= 0:
                add_info(info_dict, key, value[:idx])
                value = value[idx:]
                fix_list = re.findall(r'<th>(.*?)</th><td>(.*)', value)
                if len(fix_list) > 0:
                    key, value = fix_list[0][0], fix_list[0][1]
                    add_info(info_dict, key, value)
            else:
                add_info(info_dict, key, value)

        if len(info_dict) == 0:
            return None

        info_dict['code'] = code

        url = self.fund_info_rise_url.format(code)
        s = await self.retry_request(url)
        s1 = re.findall(r'<div class="dataOfFund">(.*?)</div>', s, re.M | re.DOTALL)
        t, r1d, net, net_acc = datetime.strptime('19000101', '%Y%m%d'), 0.0, 0.0, 0.0
        r1m, r1y, r3m, r3y, r6m, r = 0.0, 0.0, 0.0, 0.0, 0.0, 0.0
        if len(s1) > 0:
            s1 = s1[0]
            v1 = re.findall(r'单位净值.*?(\-*\d+-\d+-\d+)\).*?(\-*\d+\.\d+).*?(\-*\d+\.\d+|--).*?累计净值.*?(\-*\d+\.\d+)', s1)

            if len(v1) > 0:
                t = None if v1[0][0] == '--' else datetime.strptime(v1[0][0], '%Y-%m-%d')
                r1d = 0.0 if v1[0][2] == '--' else float(v1[0][2])
                net = 0.0 if v1[0][1] == '--' else float(v1[0][1])
                net_acc = 0.0 if v1[0][3] == '--' else float(v1[0][3])
                # t, r1d, net, net_acc = None if v1[0][0] == '--' else datetime.strptime(v1[0][0], '%Y-%m-%d'), float(v1[0][2]), float(v1[0][1]), float(v1[0][3])
                # if r1d == '--':
                #     r1d = 0.0

            res1 = r'.*?(\-*\d+.\d+|--).*?'.join(['近1月', '近1年', '近3月', '近3年', '近6月', '成立', ''])
            v1 = re.findall(res1, s1)
            if len(v1) > 0:
                r1m, r1y, r3m, r3y, r6m, r = float(0 if v1[0][0].find('--') >= 0 else v1[0][0]),\
                                             float(0 if v1[0][1].find('--') >= 0 else v1[0][1]), \
                                             float(0 if v1[0][2].find('--') >= 0 else v1[0][2]), \
                                             float(0 if v1[0][3].find('--') >= 0 else v1[0][3]), \
                                             float(0 if v1[0][4].find('--') >= 0 else v1[0][4]), \
                                             float(0 if v1[0][5].find('--') >= 0 else v1[0][5])

        s2 = re.findall(r"<div class='poptableWrap'>.*?</div>", s, re.M | re.DOTALL)
        s2 = s2[0]
        codes = re.findall(
            r'<td.*?\.com/(.*?)\.html.*?title="(.*?)".*?</td>.*?<td.*?(\d+\.\d+)%.*?<td.*?[(\d+\.\d+)%|\-\-].*?</td>.*?<td.*?</td>',
            s2)
        s_codes = []
        s_names = []
        s_repr = []
        for code in codes:
            s_code = code[0].replace('/', '')
            s_codes.append(s_code)
            s_names.append(code[1])
            s_repr.append('{}({}, {}%)'.format(code[1], s_code, code[2]))

        info_dict['date'] = t
        info_dict['net'] = net
        info_dict['net_accumulate'] = net_acc

        info_dict['rise_1d'] = r1d
        info_dict['rise_1m'] = r1m
        info_dict['rise_3m'] = r3m
        info_dict['rise_6m'] = r6m
        info_dict['rise_1y'] = r1y
        info_dict['rise_3y'] = r3y
        info_dict['rise_found'] = r

        info_dict['stock_codes'] = ','.join(s_codes)
        info_dict['stock_names'] = ','.join(s_names)
        info_dict['stock_annotate'] = ','.join(s_repr)

        frame = pd.DataFrame([info_dict])
        if fields is not None:
            frame = frame[[x.strip() for x in fields.split(',')]]
        return frame


if __name__ == '__main__':
    async def test_fund_list(f):
        frame = await f.get_fund_list()
        print(frame.head())

        frame = await f.get_fund_list(fields='code,name')
        print(frame.head())


    async def test_fund_net(f):
        # frame = await f.get_fund_net(code='160220')
        # print(frame.head())

        frame = await f.get_fund_net(code='160220', start=datetime.strptime('20191201', '%Y%m%d'),
                                     end=datetime.strptime('2020220', '%Y%m%d'), fields='code,date,net')
        print(frame.head())


    async def test_fund_info(f):
        frame = await f.get_fund_info(code='001054')
        print(frame)

    async def test_block_list(f):
        frame = await f.get_block_list()
        print(frame)


    fund = FundEastmoney()
    loop = None
    try:
        loop = asyncio.get_event_loop()
        if loop.run_until_complete(
                # test_fund_list(fund)
                # test_fund_net(fund)
                test_fund_info(fund)
                # test_block_list(fund)
        ):
            loop.run_forever()
    finally:
        if loop is not None:
            loop.close()
