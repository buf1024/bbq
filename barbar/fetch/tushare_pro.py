from functools import wraps, partial
import pandas as pd
import time
import tushare as ts
import traceback
from typing import Optional, List, Callable
import barbar.log as log
from datetime import datetime, timedelta


class TusharePro:
    """
    tushare pro 调用需要积分，积分规则不是很透明，可能以各种理由降积分。
    唯一保证积分可用的办法是捐赠，如果支持tushare pro的劳动，就捐赠获取积分，如果不愿意捐赠，慎用。

    这里高积分的接口已经删除，只留下基础120积分，最基本的部分接口。
    """

    def __init__(self, token: str):
        self.token = token
        self.api = None

        if self.token is not None:
            self.api = ts.pro_api(token)

        self.log = log.get_logger(self.__class__.__name__)

        self.max_row = 5000

    def _retry_client(func):
        @wraps(func)
        def wrapper(self, *args, **kwargs):
            for i in range(3):
                try:
                    res = func(self, *args, **kwargs)
                    return res
                except Exception as e:
                    msg = traceback.format_exc()
                    self.log.error('请求 %s tushare pro 异常: \n%s', func.__name__, msg)
                    self.log.debug('请求 %s tushare pro {}s后重试.'.format((i + 1) * 5), func.__name__)
                    time.sleep((i + 1) * 5)
            self.api = ts.pro_api(self.token)
            return None

        return wrapper

    def _loop_fetch(self, func: Callable, code: str, start: str = None, end: str = None,
                    fields: str = None) -> Optional[pd.DataFrame]:
        df = None
        while True:
            self.log.debug('_loop_fetch请求, start_date={}, end_date={}'.format(start, end))
            df2 = func(ts_code=code, start_date=start, end_date=end, fields=fields)
            if df2 is None and df2.empty:
                break
            if df is None:
                df = df2
            else:
                df = pd.concat([df, df2])

            if df2.shape[0] < self.max_row:
                break

            end = df2['trade_date'].min()
            end = datetime.strptime(end, '%Y%m%d') - timedelta(days=1)
            end = end.strftime('%Y%m%d')
        if df is not None:
            df.reset_index(drop=True, inplace=True)
        return df

    @_retry_client
    def get_trade_cal(self, start: str = None, end: str = None) -> Optional[pd.DataFrame]:
        """
        交易日历
        :param end: 'yyyymmdd'
        :param start: 'yyyymmdd'
        :return: DataFrame[cal_date,is_open]
        """
        self.log.debug('get_trade_cal交易日历请求, start={start}, end={end}'.format(start=start, end=end))
        df = self.api.trade_cal(exchange='', start_date=start, end_date=end, fields='cal_date,is_open')
        df['cal_date'] = pd.to_datetime(df['cal_date'], format='%Y%m%d')
        self.log.debug('get_trade_cal交易日历应答 size={}'.format(df.shape[0]))

        return df

    @_retry_client
    def get_code_list(self, codes: List[str] = None) -> Optional[pd.DataFrame]:
        """
        股票列表
        :return: DataFrame[code,name,area,industry,market,list_date] market->市场类型 （主板/中小板/创业板/科创板）
        """
        self.log.debug('get_code_list股票列表请求')

        df = self.api.stock_basic(list_status='L', exchange='', fields='ts_code,name')
        if df is not None and not df.empty:
            df.rename(columns={'ts_code': 'code'}, inplace=True)
        self.log.debug('get_code_list股票列表应答 size={}'.format(df.shape[0] if df is not None else 0))

        if codes is not None and df is not None:
            codes = [code.upper() for code in codes]
            cond = 'code in ["{}"]'.format("\",\"".join(codes))
            df = df.query(cond)
            df.reset_index(drop=True, inplace=True)

        return df

    @_retry_client
    def get_index_list(self, codes: List[str] = None) -> Optional[pd.DataFrame]:
        """
        指数基本信息
        :param codes:
        :return: DataFrame([code,name,market,category,index_type,exp_date])
        """
        self.log.debug('get_index_list指数基本信息请求, codes={codes}'.format(codes=codes))

        frames = []
        markets = ['SSE', 'SZSE']
        for market in markets:
            df = self.api.index_basic(market=market, fields='ts_code,name')
            if df is not None and not df.empty:
                df.rename(columns={'ts_code': 'code'}, inplace=True)
                frames.append(df)

        df = None if len(frames) == 0 else pd.concat(frames)

        self.log.debug('get_code_daily_index指数基本信息应答 size={}'.format(df.shape[0] if df is not None else 0))

        if codes is not None and df is not None:
            codes = [code.upper() for code in codes]
            cond = 'code in ["{}"]'.format("\",\"".join(codes))
            df = df.query(cond)
            df.reset_index(drop=True, inplace=True)

        return df

    @_retry_client
    def get_bar(self, code: str, frequency: str = 'D', start: str = None, end: str = None) -> Optional[pd.DataFrame]:
        """
        K线行情
        :param end:
        :param start:
        :param frequency:
        :param code: code=xxx.sh/sz
        :return: None / DataFrame([code,trade_date,open,high,low,close,vol,amt,adj_factor])
        """
        self.log.debug('get_bar K线行情请求, code={code}, frequency={frequency}, start={start}, end={end}'.
                       format(code=code, frequency=frequency, start=start, end=end))

        if frequency != 'D':
            raise NotImplementedError

        df = self._loop_fetch(self.api.daily, code=code, start=start, end=end,
                              fields='ts_code,trade_date,open,high,low,close, vol, amount')

        self.log.debug('daily K线行情应答 size={}'.format(df.shape[0] if df is not None else 0))

        if df is not None:
            df.dropna(inplace=True)
            df.drop_duplicates(inplace=True)
            df['trade_date'] = pd.to_datetime(df['trade_date'], format='%Y%m%d')
            df.rename(columns={'ts_code': 'code'}, inplace=True)
            df.reset_index(drop=True, inplace=True)

        return df

    @_retry_client
    def get_adj_factor(self, code: str, start: str = None, end: str = None) -> Optional[pd.DataFrame]:
        """
        复权因子
        :param end:
        :param start:
        :param code: code=xxx.sh/sz
        :return: DataFrame([code, trade_date, adj_factor])
        """
        self.log.debug('get_adj_factor复权因子请求, code={code}, start={start}, end={end}'.
                       format(code=code, start=start, end=end))

        if code is None:
            return None

        df = self._loop_fetch(self.api.adj_factor, code=code, start=start, end=end,
                              fields='ts_code,trade_date,adj_factor')

        self.log.debug('get_adj_factor复权因子应答 size={}'.format(df.shape[0] if df is not None else 0))

        if df is not None:
            df.dropna(inplace=True)
            df['trade_date'] = pd.to_datetime(df['trade_date'], format='%Y%m%d')
            df.rename(columns={'ts_code': 'code'}, inplace=True)
            df.reset_index(drop=True, inplace=True)

        return df


if __name__ == '__main__':
    tu = TusharePro('408481e156da6a5facd695e58add4d0bf705649fe0f460d03d4d6908')

    # df = tu.get_trade_cal()
    # print(df)

    df = tu.get_code_list()
    print(df)

    # df = tu.get_index_list(codes=['000001.SH',  # 上证综指
    #                               '399001.SZ',  # 深证成指
    #                               '399006.SZ',  # 创业板指
    #                               '399005.SZ',  # 中小板指
    #                               '000300.SH',  # 沪深300
    #                               '000688.SH',  # 科创50
    #                               '399673.SZ',  # 创业板50
    #                               '399550.SZ',  # 央视50
    #                               '399678.SZ',  # 深次新股
    #                               '399007.SZ',  # 深证300
    #                               '399008.SZ',  # 中小300
    #                               ])
    # print(df)

    # df = tu.get_bar(code='000001.SZ')
    # print(df)

    # df = tu.get_adj_factor(code='000001.SZ')
    # print(df)

    # df = tu.get_index_bar(code='399001.SZ')
    # print(df)
