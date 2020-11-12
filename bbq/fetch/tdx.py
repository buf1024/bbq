from pytdx.hq import TdxHq_API, TDXParams
from pytdx.errors import TdxFunctionCallError
from functools import wraps
from datetime import datetime
import threading
import queue
import time
import uuid
import pandas as pd
import bbq.log as log
from bbq.common import singleton
from typing import Tuple, Any, Optional, List, Dict
import traceback


@singleton
class Tdx(threading.Thread):
    """
    pytdx 已经不维护，期间出现的BUG也不修复。
    返回数据的条目依赖于不同的ip，不同ip同样请求，返回的数据可能不一致。

    而且历史数据不一定全。可做年内数据的回测，年久数据不一定获取到到，如1m~60mk线。
    实时行情或其他一些接口可用。
    """
    hq_hosts = [
        # ("长城国瑞电信1", "218.85.139.19", 7709),
        # ("长城国瑞电信2", "218.85.139.20", 7709),
        ("长城国瑞网通", "58.23.131.163", 7709),
        ("上证云成都电信一", "218.6.170.47", 7709),
        ("上证云北京联通一", "123.125.108.14", 7709),
        ("上海电信主站Z1", "180.153.18.170", 7709),
        ("上海电信主站Z2", "180.153.18.171", 7709),
        ("上海电信主站Z80", "180.153.18.172", 80),
        # ("北京联通主站Z1", "202.108.253.130", 7709),
        ("北京联通主站Z2", "202.108.253.131", 7709),
        ("北京联通主站Z80", "202.108.253.139", 80),
        ("杭州电信主站J1", "60.191.117.167", 7709),
        ("杭州电信主站J2", "115.238.56.198", 7709),
        ("杭州电信主站J3", "218.75.126.9", 7709),
        ("杭州电信主站J4", "115.238.90.165", 7709),
        ("杭州联通主站J1", "124.160.88.183", 7709),
        ("杭州联通主站J2", "60.12.136.250", 7709),
        ("杭州华数主站J1", "218.108.98.244", 7709),
        ("杭州华数主站J2", "218.108.47.69", 7709),
        # ("义乌移动主站J1", "223.94.89.115", 7709),
        # ("青岛联通主站W1", "218.57.11.101", 7709),
        # ("青岛电信主站W1", "58.58.33.123", 7709),
        # ("深圳电信主站Z1", "14.17.75.71", 7709),
        ("云行情上海电信Z1", "114.80.63.12", 7709),
        ("云行情上海电信Z2", "114.80.63.35", 7709),
        ("上海电信主站Z3", "180.153.39.51", 7709),
        ('招商证券深圳行情', '119.147.212.81', 7709),
        # ('华泰证券(南京电信)', '221.231.141.60', 7709),
        # ('华泰证券(上海电信)', '101.227.73.20', 7709),
        # ('华泰证券(上海电信二)', '101.227.77.254', 7709),
        # ('华泰证券(深圳电信)', '14.215.128.18', 7709),
        # ('华泰证券(武汉电信)', '59.173.18.140', 7709),
        # ('华泰证券(天津联通)', '60.28.23.80', 7709),
        # ('华泰证券(沈阳联通)', '218.60.29.136', 7709),
        # ('华泰证券(南京联通)', '122.192.35.44', 7709),
        # ('华泰证券(南京联通)', '122.192.35.44', 7709),
        # ('安信', '112.95.140.74', 7709),
        # ('安信', '112.95.140.92', 7709),
        # ('安信', '112.95.140.93', 7709),
        ('安信', '114.80.149.19', 7709),
        ('安信', '114.80.149.21', 7709),
        ('安信', '114.80.149.22', 7709),
        ('安信', '114.80.149.91', 7709),
        ('安信', '114.80.149.92', 7709),
        ('安信', '121.14.104.60', 7709),
        ('安信', '121.14.104.66', 7709),
        # ('安信', '123.126.133.13', 7709),
        # ('安信', '123.126.133.14', 7709),
        # ('安信', '123.126.133.21', 7709),
        # ('安信', '211.139.150.61', 7709),
        ('安信', '59.36.5.11', 7709),
        ('广发', '119.29.19.242', 7709),
        ('广发', '123.138.29.107', 7709),
        ('广发', '123.138.29.108', 7709),
        # ('广发', '124.232.142.29', 7709),
        ('广发', '183.57.72.11', 7709),
        # ('广发', '183.57.72.12', 7709),
        ('广发', '183.57.72.13', 7709),
        ('广发', '183.57.72.15', 7709),
        # ('广发', '183.57.72.21', 7709),
        ('广发', '183.57.72.22', 7709),
        # ('广发', '183.57.72.23', 7709),
        ('广发', '183.57.72.24', 7709),
        ('广发', '183.60.224.177', 7709),
        ('广发', '183.60.224.178', 7709),
        ('国泰君安', '117.34.114.13', 7709),
        ('国泰君安', '117.34.114.14', 7709),
        ('国泰君安', '117.34.114.15', 7709),
        ('国泰君安', '117.34.114.16', 7709),
        ('国泰君安', '117.34.114.17', 7709),
        ('国泰君安', '117.34.114.18', 7709),
        ('国泰君安', '117.34.114.20', 7709),
        ('国信', '58.63.254.247', 7709),
        ('海通', '123.125.108.90', 7709),
        ('海通', '175.6.5.153', 7709),
        ('海通', '182.118.47.151', 7709),
        ('海通', '182.131.3.245', 7709),
        ('海通', '202.100.166.27', 7709),
        ('海通', '58.63.254.191', 7709),
        ('海通', '58.63.254.217', 7709),
        ('华林', '202.100.166.21', 7709),
        ('华林', '202.96.138.90', 7709),
        ('华林', '218.106.92.182', 7709),
        ('华林', '218.106.92.183', 7709),
        ('华林', '220.178.55.71', 7709),
        ('华林', '220.178.55.86', 7709),

        # 以下IP经常断
        # ("长城国瑞电信1", "218.85.139.19", 7709),
        # ("长城国瑞电信2", "218.85.139.20", 7709),
        # ("北京联通主站Z1", "202.108.253.130", 7709),
        # ("义乌移动主站J1", "223.94.89.115", 7709),
        # ("青岛联通主站W1", "218.57.11.101", 7709),
        # ("青岛电信主站W1", "58.58.33.123", 7709),
        # ("深圳电信主站Z1", "14.17.75.71", 7709),
        # ('华泰证券(南京电信)', '221.231.141.60', 7709),
        # ('华泰证券(上海电信)', '101.227.73.20', 7709),
        # ('华泰证券(上海电信二)', '101.227.77.254', 7709),
        # ('华泰证券(深圳电信)', '14.215.128.18', 7709),
        # ('华泰证券(武汉电信)', '59.173.18.140', 7709),
        # ('华泰证券(天津联通)', '60.28.23.80', 7709),
        # ('华泰证券(沈阳联通)', '218.60.29.136', 7709),
        # ('华泰证券(南京联通)', '122.192.35.44', 7709),
        # ('华泰证券(南京联通)', '122.192.35.44', 7709),
        # ('安信', '112.95.140.74', 7709),
        # ('安信', '112.95.140.92', 7709),
        # ('安信', '112.95.140.93', 7709),
        # ('安信', '123.126.133.13', 7709),
        # ('安信', '123.126.133.14', 7709),
        # ('安信', '123.126.133.21', 7709),
        # ('安信', '211.139.150.61', 7709),
        # ('广发', '124.232.142.29', 7709),
        # ('广发', '183.57.72.12', 7709),
        # ('广发', '183.57.72.21', 7709),
        # ('广发', '183.57.72.23', 7709),
        # ('国泰君安', '113.105.92.100', 7709),
        # ('国泰君安', '113.105.92.101', 7709),
        # ('国泰君安', '113.105.92.102', 7709),
        # ('国泰君安', '113.105.92.103', 7709),
        # ('国泰君安', '113.105.92.104', 7709),
        # ('国泰君安', '113.105.92.99', 7709),
        # ('国泰君安', '117.34.114.27', 7709),
        # ('国泰君安', '117.34.114.30', 7709),
        # ('国泰君安', '117.34.114.31', 7709),
        # ('国信', '182.131.3.252', 7709),
        # ('国信', '183.60.224.11', 7709),
        # ('国信', '58.210.106.91', 7709),
        # ('国信', '58.63.254.216', 7709),
        # ('国信', '58.63.254.219', 7709),
        # ('海通', '222.161.249.156', 7709),
        # ('海通', '42.123.69.62', 7709),
        # ('华林', '120.55.172.97', 7709),
        # ('华林', '139.217.20.27', 7709),
    ]

    def __init__(self):
        threading.Thread.__init__(self, daemon=True)

        self.queue = queue.Queue()
        self.lock = threading.Lock()
        self.init_queue = queue.Queue()

        self.clients = []

        self.hosts = [(h, p, 0) for _, h, p in self.hq_hosts]

        self.log = log.get_logger(self.__class__.__name__)

        self.inited = None
        self.max_row = 800

    def run(self):
        self.log.debug('detect ip ...')
        while True:
            try:
                hosts = self.queue.get(timeout=1)
                if hosts is None:
                    break
                is_put = False
                for host in hosts:
                    self.reconnect(host=host)
                    if self.init_queue is not None and len(self.clients) > 0:
                        self.init_queue.put(self.clients)
                        is_put = True
                if not is_put:
                    if self.init_queue is not None:
                        self.init_queue.put(self.clients)
            except queue.Empty:
                if len(self.clients) > 0:
                    break
            except:
                pass

        self.log.debug('detect ip done, available={}'.format(len(self.clients)))

    def reconnect(self, host: Tuple):
        api = TdxHq_API(raise_exception=True, auto_retry=True,
                        multithread=True, heartbeat=True)
        try:
            retry = host[2]
            if retry > 15:
                self.log.warning('discard %s:%d after 3 retries', host[0], host[1])
                return
            api.connect(host[0], host[1])

            s = time.time()
            api.get_security_count(0)
            e = time.time()

            with self.lock:
                self.clients.append({'id': uuid.uuid4(), 'client': api, 'time': int(e - s),
                                     'last': e, 'count': 1, 'host': host[0], 'port': host[1],
                                     'retry': host[2]})
                self.clients = sorted(self.clients, key=lambda x: (x['time'], x['last'], x['count']))
        except Exception as e:
            try:
                api.disconnect()
            except:
                pass
            self.log.warning('connecting %s:%d timeout', host[0], host[1])

    def _best_client(func):
        @wraps(func)
        def wrapper(self, *args, **kwargs):
            for i in range(3):
                stat_client = None
                with self.lock:
                    stat_client = self.clients[0] if len(self.clients) > 0 else None

                if stat_client is None:
                    return None
                try:
                    kwargs['__client'] = stat_client['client'] if stat_client is not None else None
                    s = time.time()
                    res = func(self, *args, **kwargs)
                    e = time.time()
                    if stat_client is not None:
                        stat_client['time'] = int(e - s)
                        stat_client['last'] = e
                        stat_client['count'] = stat_client['count'] + 1
                        with self.lock:
                            self.clients = sorted(self.clients, key=lambda x: (x['time'], x['last'], x['count']))
                    return res
                except TdxFunctionCallError as e:
                    with self.lock:
                        for idx, v in enumerate(self.clients):
                            if v['id'] == stat_client['id']:
                                del self.clients[idx]
                                self.reconnect((v['host'], v['port'], v['retry'] + 1))
                                break
                    self.log.error('请求 %s tdx 异常: \n%s', func.__name__, e)

            return None

        return wrapper

    def init(self):
        if self.inited:
            return self.inited

        self.queue.put(self.hosts)
        self.start()

        try:
            clients = self.init_queue.get(timeout=60)
            with self.lock:
                self.init_queue = None

            self.inited = len(clients) > 0
        except:
            self.inited = False

        return self.inited

    def stop(self):
        with self.lock:
            for client in self.clients:
                client['client'].disconnect()
        # self.queue.put(None)

    @staticmethod
    def _market_code(code: str) -> Tuple:
        market = 0 if code[-2:].upper() == 'SZ' else 1
        code = code[:-3]
        return market, code

    @staticmethod
    def _frequency_type(frequency: str = 'D') -> int:
        #  K线种类
        # 0 5分钟K线 1 15分钟K线 2 30分钟K线 3 1小时K线 4 日K线
        # 5 周K线
        # 6 月K线
        # 7 1分钟
        # 8 1分钟K线 9 日K线
        # 10 季K线
        # 11 年K线

        frequency = frequency.upper()
        typ = 9
        if frequency in ['1D', 'D', 'DAY']:
            typ = 9
        if frequency in ['1W', 'W', 'WEEK']:
            typ = 5
        if frequency in ['MON', 'MONTH']:
            typ = 6
        if frequency in ['1Q', 'Q', 'QUARTER']:
            typ = 10
        if frequency in ['1Y', 'Y', 'YEAR']:
            typ = 11

        if frequency in ['M', '1M', '1MIN', 'MINUTE']:
            typ = 8
        if frequency in ['5M', '5MIN', '5MINUTE']:
            typ = 0
        if frequency in ['15M', '15MIN', '15MINUTE']:
            typ = 1
        if frequency in ['30M', '30MIN', '30MINUTE']:
            typ = 2
        if frequency in ['60M', '60MIN', '60MINUTE', '1H', 'HOUR']:
            typ = 3

        return typ


    @_best_client
    def get_code_list(self, codes: List[str] = None, **kwargs) -> Optional[pd.DataFrame]:
        """
        股票列表
        :return: None / DataFrame[code name]
        """
        api = kwargs['__client']
        if api is None:
            return None

        df = None
        for i in range(2):
            offset = 0
            while True:
                df2 = api.to_df(api.get_security_list(i, offset))
                if df2 is None or df2.empty:
                    break
                offset = offset + df2.shape[0]
                df2 = df2[
                    df2['code'].str.startswith('30') |
                    df2['code'].str.startswith('00') |
                    df2['code'].str.startswith('60') |
                    df2['code'].str.startswith('68')]

                if df2 is not None and not df2.empty:
                    df2['code'] = df2['code'] + '.SZ' if i == 0 else df2['code'] + '.SH'
                    df2 = df2[['code', 'name']]

                if df is None:
                    df = df2
                else:
                    if df2 is not None and not df2.empty:
                        df = pd.concat([df, df2])

        if codes is not None and df is not None:
            codes = [code.upper() for code in codes]
            cond = 'code in ["{}"]'.format("\",\"".join(codes))
            df = df.query(cond)
            if df is not None and not df.empty:
                df.reset_index(drop=True, inplace=True)

        return df

    @_best_client
    def _get_bar(self, market_type: str, code: str, frequency: str = 'D', start: str = None, end: str = None, **kwargs) \
            -> Optional[pd.DataFrame]:
        """
        K线行情
        :param end:
        :param start:
        :param frequency:
        :param code:
        :return: DataFrame([code,trade_date,open,high,low,close,vol,amount, datetime])
        """

        api = kwargs['__client']
        if api is None or code is None:
            return None

        df = None
        orig_code = code
        market, code = self._market_code(code)

        typ = self._frequency_type(frequency=frequency)
        start_date, end_date = None, None
        if start is not None:
            if typ in [0, 1, 2, 3, 8]:
                start_date = datetime.strptime(start, '%Y%m%d %H:%M:%S')
            else:
                start_date = datetime.strptime(start + ' 15:00', '%Y%m%d %H:%M')

        if end is not None:
            if typ in [0, 1, 2, 3, 8]:
                end_date = datetime.strptime(end, '%Y%m%d %H:%M:%S')
            else:
                end_date = datetime.strptime(end + ' 15:00', '%Y%m%d %H:%M')

        if start_date is not None and end_date is not None:
            if end_date < start_date:
                return None

        offset = 0
        while True:
            func = api.get_security_bars if market_type == 'S' else api.get_index_bars
            df2 = api.to_df(func(typ, market, code, offset, self.max_row))
            if df2 is None or df2.empty:
                break

            df2['code'] = orig_code.upper()
            df2_time = df2['year'].map(str) + '-' + df2['month'].map(str) + '-' + df2['day'].map(str)
            df2['trade_date'] = df2_time.apply(pd.to_datetime, format='%Y-%m-%d')

            df2['datetime'] = pd.to_datetime(df2['datetime'], format='%Y-%m-%d %H:%M')

            df2 = df2[['code', 'trade_date', 'open', 'high', 'low', 'close', 'vol', 'amount', 'datetime']]
            offset = offset + df2.shape[0]

            if df is None:
                df = df2
            else:
                df = pd.concat([df2, df])

            if start_date is not None:
                min_date = df['datetime'].min()
                if min_date < start_date:
                    break
        if df is not None:
            if start_date is not None:
                df = df[df['datetime'] >= start_date]

            if end_date is not None and df is not None and not df.empty:
                df = df[df['datetime'] <= end_date]

            if df is not None and not df.empty:
                if typ not in [0, 1, 2, 3, 8]:
                    df.drop(['datetime'], axis=1, inplace=True)
                df.reset_index(drop=True, inplace=True)

        return df

    def get_bar(self, code: str, frequency: str = 'D', start: str = None, end: str = None) \
            -> Optional[pd.DataFrame]:
        """
        :param end:
        :param start:
        :param frequency:
        :param code:
        :return: DataFrame([code,trade_date,open,high,low,close,vol,amount, datetime])
        """
        self.log.debug('get_bar K线行情请求, code={code}, frequency={frequency}, start={start}, end={end} ...'.
                       format(code=code, frequency=frequency, start=start, end=end))
        df = self._get_bar(market_type='S', code=code, frequency=frequency, start=start, end=end)
        self.log.debug('get_bar K线行情应答, size={}'.format(df.shape[0] if df is not None else 0))
        return df

    def get_index_bar(self, code: str, frequency: str = 'D', start: str = None, end: str = None) \
            -> Optional[pd.DataFrame]:
        """
        :param end:
        :param start:
        :param frequency:
        :param code:
        :return: DataFrame([code,trade_date,open,high,low,close,vol,amount, datetime])
        """
        self.log.debug('get_index_bar K线行情请求, code={code}, frequency={frequency}, start={start}, end={end} ...'.
                       format(code=code, frequency=frequency, start=start, end=end))
        df = self._get_bar(market_type='I', code=code, frequency=frequency, start=start, end=end)
        self.log.debug('get_index_bar K线行情应答, size={}'.format(df.shape[0] if df is not None else 0))
        return df

    @_best_client
    def get_rt_quot(self, codes: List[str], **kwargs) -> Optional[Dict[str, Dict[str, Any]]]:
        """

        :param codes:
        :return:
        [code: {
            name=xxx,
            open=xx,pre_close=xx,now=xx,high=xx,low=xx,buy=xx,sell=xx,
            vol=xx, amount=xx, # 累计成交量、成交额
            bid=[(bid1_vol, bid1_price), ...], ask=[(ask1_vol, ask1_price), ...],
            date=yyyymmdd,time=hh:mm:ss}, ...]/None,
        """
        self.log.debug('get_rt_quot 实时行情请求, codes={}'.format(codes))
        api = kwargs['__client']
        if api is None:
            return None

        param = [self._market_code(code) for code in codes]
        quote = api.get_security_quotes(param)
        quot_dict = {}
        for i in range(len(codes)):
            q = quote[i]
            quot_dict[codes[i]] = dict(
                code=codes[i],
                name='',
                open=float(q['open']), pre_close=float(q['last_close']),
                now=float(q['price']), high=float(q['high']), low=float(q['low']),
                buy=float(q['bid1']), sell=float(q['ask1']),
                vol=int(q['vol']), amount=float(q['amount']),
                bid=[(int(q['bid1']), float(q['bid_vol1'])), (int(q['bid2']), float(q['bid_vol2'])),
                     (int(q['bid3']), float(q['bid_vol3'])), (int(q['bid4']), float(q['bid_vol4'])),
                     (int(q['bid5']), float(q['bid_vol5']))],
                ask=[(int(q['ask1']), float(q['ask_vol1'])), (int(q['ask2']), float(q['ask_vol2'])),
                     (int(q['ask3']), float(q['ask_vol3'])), (int(q['ask4']), float(q['ask_vol4'])),
                     (int(q['ask5']), float(q['ask_vol5']))],
                date=datetime.now(),
                datetime=datetime.strptime(
                    datetime.now().strftime('%Y-%m-%d') + ' ' + q['servertime'].split('.')[0], '%Y-%m-%d %H:%M:%S'), )

        self.log.debug('get_rt_quot 实时行情应答, size={}'.format(len(quote) if quote is not None else 0))
        return quot_dict

    @_best_client
    def get_block_list(self, **kwargs) -> Optional[pd.DataFrame]:
        """

        :return: DataFrame[blockname, code]
        """
        self.log.debug('get_block_list 板块信息请求')
        api = kwargs['__client']
        if api is None:
            return None

        df = None
        for block in [TDXParams.BLOCK_DEFAULT, TDXParams.BLOCK_SZ, TDXParams.BLOCK_GN, TDXParams.BLOCK_FG]:
            df2 = api.to_df(api.get_and_parse_block_info(block))
            if df2 is None or df2.empty:
                continue
            df2 = df2.groupby('blockname').code.apply(list)
            df2 = df2.reset_index()

            if df is None:
                df = df2
            else:
                df = pd.concat([df, df2])

        self.log.debug('get_block_list 板块信息应答, size={}'.format(df.shape[0] if df is not None else 0))
        return df

    @_best_client
    def get_xdxr_list(self, code: str, **kwargs) -> Optional[pd.DataFrame]:
        """

        :param code:
        :return:
        """
        self.log.debug('get_xdxr_list 除权除息信息请求, code={} ...'.format(code))
        api = kwargs['__client']
        if api is None:
            return None
        orig_code = code
        market, code = self._market_code(code)

        category = {
            '1': '除权除息', '2': '送配股上市', '3': '非流通股上市', '4': '未知股本变动',
            '5': '股本变化',
            '6': '增发新股', '7': '股份回购', '8': '增发新股上市', '9': '转配股上市',
            '10': '可转债上市',
            '11': '扩缩股', '12': '非流通股缩股', '13': '送认购权证', '14': '送认沽权证'}
        df = api.to_df(api.get_xdxr_info(market, code))
        if df is not None and not df.empty:
            df['code'] = orig_code.upper()
            df['date'] = pd.to_datetime(df[['year', 'month', 'day']])
            df['category_mean'] = df['category'].apply(lambda x: category[str(x)])
            df.drop(['year', 'month', 'day'], axis=1, inplace=True)
            df.reset_index(drop=True, inplace=True)

        self.log.debug('get_xdxr_list 除权除息信息应答, size={} ...'.format(df.shape[0] if df is not None else 0))
        return df


if __name__ == '__main__':
    tdx = Tdx()
    tdx.init()

    # df = tdx.get_code_list()
    # print(df)

    # df = tdx.get_bar(code='000008.SH', start=None, end='20200829', frequency='5min')
    # print(df)

    # df = tdx.get_index_bar(code='399001.sz', frequency='60m')
    # print(df)

    # df = tdx.get_block_list()
    # print(df)

    # df = tdx.get_xdxr_list('000002.Sz')
    # print(df)

    df = tdx.get_rt_quot(['000002.Sz', '000001.sz'])
    print(df)
