from datetime import datetime, timedelta
from typing import Optional, List

import akshare as ak
from opendatatools import stock
import pandas as pd

from bbq.fetch.base_fetch import BaseFetch

import xlrd

# 参考： https://stackoverflow.com/questions/64264563/attributeerror-elementtree-object-has-no-attribute-getiterator-when-trying
# python3.9
# xml.etree.ElementTree.Element.getiterator() has been deprecated since Python 2.7,
# and has been removed in Python 3.9. Replace all instances of Element.getiterator(tag)
# with Element.iter(tag)
# 补丁
xlrd.xlsx.ensure_elementtree_imported(False, None)
xlrd.xlsx.Element_has_iter = True


class MyFetch(BaseFetch):
    def __init__(self):
        super().__init__()

    @BaseFetch.retry_client
    def fetch_stock_info(self, codes: List[str] = None) -> Optional[pd.DataFrame]:
        """
        股票信息
        :param codes:
        :return:
        """
        self.log.debug('获取上证股票...')
        df = ak.stock_info_sh_name_code(indicator="主板A股")
        if df is None:
            self.log.debug('上证 主板A股 失败')
            return None
        df['block'] = '主板'

        df2 = ak.stock_info_sh_name_code(indicator="科创板")
        if df2 is None:
            self.log.error('上证 科创板 失败')
            return None
        df2['block'] = '科创板'
        df = df.append(df2)

        df['code'] = df['SECURITY_CODE_A'].apply(lambda x: 'sh' + x)
        df['listing_date'] = pd.to_datetime(df['LISTING_DATE'], format='%Y-%m-%d')
        data = df.rename(columns={'SECURITY_ABBR_A': 'name'})[['code', 'name', 'listing_date', 'block']]

        self.log.debug('获取深证股票...')
        df = ak.stock_info_sz_name_code(indicator="主板")
        if df is None:
            self.log.error('深证 主板 失败')
            return None
        df['block'] = '主板'
        df.dropna(subset=['A股代码'], inplace=True)
        df.dropna(subset=['A股简称'], inplace=True)
        df = df[df['A股代码'] != '000nan']
        df['A股代码'] = df['公司代码'].astype(str).str.zfill(6)

        df2 = ak.stock_info_sz_name_code(indicator="中小企业板")
        if df2 is None:
            self.log.error('深证 中小企业板 失败')
            return None
        df2['block'] = '中小板'

        df = df.append(df2)

        df2 = ak.stock_info_sz_name_code(indicator="创业板")
        if df2 is None:
            self.log.error('深证 创业板 失败')
            return None
        df2['block'] = '创业板'
        df = df.append(df2)

        df['code'] = df['A股代码'].apply(lambda x: 'sz' + x)
        df['listing_date'] = pd.to_datetime(df['A股上市日期'], format='%Y-%m-%d')
        data2 = df.rename(columns={'公司简称': 'name'})[['code', 'name', 'listing_date', 'block']]
        data = data.append(data2)

        if codes is not None and data is not None:
            cond = 'code in ["{}"]'.format("\",\"".join(codes))
            data = data.query(cond)

        data = data.reindex()
        self.log.debug('获取股票成功, count={}'.format(data.shape[0]))
        return data

    @BaseFetch.retry_client
    def fetch_stock_adj_factor(self, code: str) -> Optional[pd.DataFrame]:
        try:
            self.log.debug('获取股票{}后复权因子...'.format(code))
            df_hfq_factor = ak.stock_zh_a_daily(symbol=code, adjust='hfq-factor')
            if df_hfq_factor is None:
                self.log.error('获取股票{}后复权因子失败'.format(code))
                return None
            df_hfq_factor.dropna(inplace=True)

            self.log.debug('获取股票{}前复权因子...'.format(code))
            df_qfq_factor = ak.stock_zh_a_daily(symbol=code, adjust='qfq-factor')
            if df_qfq_factor is None:
                self.log.error('获取股票{}前复权因子失败'.format(code))
                return None
            df_qfq_factor.dropna(inplace=True)
            df = df_hfq_factor.merge(df_qfq_factor, how='left', left_on=['date'], right_on=['date'])
            df = df.reset_index()
            df['code'] = code
            df.rename(columns={'date': 'trade_date'}, inplace=True)
            df = df.reindex()
            return df
        except Exception as e:
            self.log.info('获取股票{}复权数据失败(可能未曾复权)'.format(code))

        return None

    @BaseFetch.retry_client
    def fetch_fund_daily_xueqiu(self, code: str, start: datetime = None, end: datetime = None) -> Optional[
        pd.DataFrame]:
        if not self.is_trade(start, end):
            self.log.info('code={}, start={}, end={}, 非交易日不同步...'.format(code, start, end))
            return None
        self.log.debug('获取雪球场内基金{}日线数据, start={}, end={}...'.format(code, start, end))
        if start is None:
            self.log.error('开始日期不能为空')
            return None

        if end is None:
            now = datetime.now()
            end = now
            if now < datetime(year=now.year, month=now.month, day=now.day, hour=15, minute=30):
                end = now - timedelta(days=1)

        start_date, end_date = start.strftime('%Y-%m-%d'), end.strftime('%Y-%m-%d')
        df, msg = stock.get_daily(symbol=self.fund2xueqiu(code), start_date=start_date, end_date=end_date)
        if df is not None:
            df.dropna(inplace=True)
            df.rename(columns={'last': 'close', 'turnover_rate': 'turnover'}, inplace=True)
            df['turnover'] = df['turnover']
            df['trade_date'] = pd.to_datetime(df['time'], format='%Y-%m-%d')
            df.drop(columns=['symbol', 'change', 'percent', 'time'], inplace=True)
            df['code'] = code
        else:
            self.log.error('获取基金{}日线数据失败: {}'.format(code, msg))
            return None
        self.log.debug('获取雪球场内基金{}日线数据, count={}'.format(code, df.shape[0]))
        return df

    @BaseFetch.retry_client
    def fetch_stock_daily_xueqiu(self, code: str, start: datetime = None, end: datetime = None) -> Optional[
        pd.DataFrame]:
        if not self.is_trade(start, end):
            self.log.info('code={}, start={}, end={}, 非交易日不同步...'.format(code, start, end))
            return None
        self.log.debug('获取雪球股票{}日线数据, start={}, end={}...'.format(code, start, end))
        if start is None:
            code_info = self.fetch_stock_info(codes=[code])
            if code_info is None or code_info.empty:
                self.log.debug('获取股票{}日线数据失败: 无股票信息'.format(code))
                return None
            start = code_info.iloc[0]['listing_date']
        if end is None:
            now = datetime.now()
            end = now
            if now < datetime(year=now.year, month=now.month, day=now.day, hour=15, minute=30):
                end = now - timedelta(days=1)

        start_date, end_date = start.strftime('%Y-%m-%d'), end.strftime('%Y-%m-%d')
        df, msg = stock.get_daily(symbol=self.sina2xueqiu(code), start_date=start_date, end_date=end_date)
        if df is not None:
            df.dropna(inplace=True)
            df.rename(columns={'last': 'close', 'turnover_rate': 'turnover'}, inplace=True)
            df['turnover'] = df['turnover']
            df['date'] = pd.to_datetime(df['time'], format='%Y-%m-%d')
            df.drop(columns=['symbol', 'change', 'percent', 'time'], inplace=True)
        else:
            self.log.error('获取股票{}未复权数据失败: {}'.format(code, msg))
            return None
        self.log.debug('获取雪球股票{}日线数据, count={}'.format(code, df.shape[0]))
        return df

    @BaseFetch.retry_client
    def fetch_stock_daily(self, code: str, start: datetime = None, end: datetime = None, adjust: bool = True) -> Optional[pd.DataFrame]:
        """
        股票日线数据,
        :param adjust:
        :param code:
        :param start:
        :param end:
        :return:
        """
        if not self.is_trade(start, end):
            self.log.debug('code={}, start={}, end={}, 非交易日不同步...'.format(code, start, end))
            return None

        df_hfq_factor = None
        if adjust:
            try:
                self.log.debug('获取股票{}后复权因子...'.format(code))
                df_hfq_factor = ak.stock_zh_a_daily(symbol=code, adjust='hfq-factor')
                if df_hfq_factor is None:
                    self.log.error('获取股票{}后复权因子失败'.format(code))
                    return None
                df_hfq_factor.dropna(inplace=True)
            except Exception as e:
                self.log.info('获取股票{}复权数据失败(可能未曾复权)'.format(code))
                df_hfq_factor = None

        self.log.debug('获取股票{}未复权数据...'.format(code))
        df = None
        if start is not None and end is not None:
            # start_date = df_hfq_factor.index[0]
            # start_date = datetime(year=start_date.year, month=start_date.month, day=start_date.day)
            # if start < start_date:
            #     start_date = start
            # end_date = end
            df = self.fetch_stock_daily_xueqiu(code, start=start, end=end)

        if df is None:
            try:
                df = ak.stock_zh_a_daily(symbol=code)
                if df is not None:
                    df.dropna(inplace=True)
                    df.drop(columns=['outstanding_share'], inplace=True)
                    df['turnover'] = df['turnover'].apply(lambda x: round(x * 100, 2))
                    df.reset_index(inplace=True)
            except Exception as e:
                self.log.error('新浪获取日线失败, 尝试雪球')
                df = self.fetch_stock_daily_xueqiu(code, start=start, end=end)

        if df is None:
            self.log.error('获取股票{}未复权数据失败'.format(code))
            return None

        if df.empty:
            self.log.debug('获取股票{}日线数据, count={}'.format(code, df.shape[0]))
            return None

        if df_hfq_factor is not None:

            # 后复权上市日往后复权，上市当日复权因子为1.0 数据不会变更, 可以填充返回
            df = df.merge(df_hfq_factor, how='left', left_on=['date'], right_on=['date'])
            df.fillna(method='ffill', inplace=True)

            # df 有最旧到最新
            # 区间查询 可能复权不到
            if df.iloc[0].isna()['hfq_factor']:
                df['hfq_factor'].fillna(value=df_hfq_factor.iloc[0]['hfq_factor'], inplace=True)

            # 前复权当日往上市日复权，当日复权因子为1.0 数据会变更, 不用填充返回
            # df = df.merge(df_qfq_factor, how='left', left_on=['date'], right_on=['date'])
            # df.fillna(method='bfill', inplace=True)
            # df.fillna(method='ffill', inplace=True)
        else:
            # df['qfq_factor'] = np.nan
            df['hfq_factor'] = 1.0

        df['code'] = code

        if start is not None:
            date_str = start.strftime('%Y-%m-%d')
            cond = 'date >= "{}"'.format(date_str)
            df = df.query(cond)
        if end is not None:
            date_str = end.strftime('%Y-%m-%d')
            cond = 'date <= "{}"'.format(date_str)
            df = df.query(cond)

        df = df.reindex()
        df.rename(columns={'date': 'trade_date'}, inplace=True)
        self.log.debug('获取股票{}日线数据, count={}'.format(code, df.shape[0]))

        return df

    @BaseFetch.retry_client
    def fetch_stock_index(self, code: str, start: datetime = None, end: datetime = None) -> Optional[pd.DataFrame]:
        """
        股票指标数据
        :param code:
        :param start:
        :param end:
        :return:
        """
        if not self.is_trade(start, end):
            self.log.info('code={}, start={}, end={}, 非交易日不同步...'.format(code, start, end))
            return None

        self.log.debug('获取股票{}指标数据...'.format(code))
        code_ak = code
        if code.startswith('sh') or code.startswith('sz'):
            code_ak = code[2:]
        df = ak.stock_a_lg_indicator(stock=code_ak)
        if df is None:
            self.log.error('获取股票{}指标数据失败'.format(code))
            return None
        df['code'] = code

        if start is not None:
            date_str = start.strftime('%Y-%m-%d')
            cond = 'trade_date >= "{}"'.format(date_str)
            df = df.query(cond)
        if end is not None:
            date_str = end.strftime('%Y-%m-%d')
            cond = 'trade_date <= "{}"'.format(date_str)
            df = df.query(cond)

        df = df.reindex()

        self.log.debug('获取股票{}指标数据, count={}'.format(code, df.shape[0]))
        return df

    @BaseFetch.retry_client
    def fetch_index_daily(self, code: str, start: datetime = None, end: datetime = None) -> Optional[pd.DataFrame]:
        """
        指数日线数据
        :param code:
        :param start:
        :param end:
        :return:
        """
        if not self.is_trade(start, end):
            self.log.info('code={}, start={}, end={}, 非交易日不同步...'.format(code, start, end))
            return None
        self.log.debug('获取指数{}日线数据...'.format(code))
        df = None
        if start is not None or end is not None:
            start_date = start.strftime('%Y-%m-%d') if start is not None else '1990-01-01'
            end_date = end.strftime('%Y-%m-%d') if end is not None else datetime.now().strftime('%Y-%m-%d')
            delta = datetime.strptime(end_date, '%Y-%m-%d') - datetime.strptime(start_date, '%Y-%m-%d')
            if delta.days <= 365:
                mk, symbol = code[:2], code[2:]
                symbol = symbol + '.SH' if mk == 'sh' else symbol + '.SZ'
                df, msg = stock.get_daily(symbol=symbol, start_date=start_date, end_date=end_date)
                if df is not None:
                    df.rename(columns={'last': 'close'}, inplace=True)
                    df['date'] = pd.to_datetime(df['time'], format='%Y-%m-%d')
                    df.drop(columns=['symbol', 'change', 'percent', 'time', 'turnover_rate'], inplace=True)
                else:
                    self.log.error('获取指数{}日线数据失败: {}'.format(code, msg))
        if df is None:
            df = ak.stock_zh_index_daily(symbol=code)
            if df is not None:
                df.reset_index(inplace=True)

        if df is None:
            self.log.error('获取指数{}日线数据失败'.format(code))
            return None
        df['code'] = code

        if start is not None:
            date_str = start.strftime('%Y-%m-%d')
            cond = 'date >= "{}"'.format(date_str)
            df = df.query(cond)
        if end is not None:
            date_str = end.strftime('%Y-%m-%d')
            cond = 'date <= "{}"'.format(date_str)
            df = df.query(cond)
        df.rename(columns={'date': 'trade_date'}, inplace=True)
        self.log.debug('获取指数{}日线数据, count={}'.format(code, df.shape[0]))
        return df

    @BaseFetch.retry_client
    def fetch_stock_minute(self, code: str, period: str, adjust: str = '', start: datetime = None,
                           end: datetime = None) -> Optional[pd.DataFrame]:
        """
        股票分钟数据
        :param code:
        :param period:
        :param adjust:
        :param start:
        :param end:
        :return:
        """
        if not self.is_trade(start, end):
            self.log.info('code={}, start={}, end={}, 非交易日不同步...'.format(code, start, end))
            return None
        self.log.debug('获取股票{} {}分钟数据...'.format(code, period))
        df = ak.stock_zh_a_minute(symbol=code, period=period, adjust=adjust)
        if df is None:
            self.log.error('获取股票{} {}分钟数据失败'.format(code, period))
            return None

        df['code'] = code

        if start is not None:
            date_str = start.strftime('%Y-%m-%d %H:%M:%S')
            cond = 'day >= "{}"'.format(date_str)
            df = df.query(cond)
        if end is not None:
            end = end + timedelta(days=1)
            date_str = end.strftime('%Y-%m-%d %H:%M:%S')
            cond = 'day < "{}"'.format(date_str)
            df = df.query(cond)

        df['day_time'] = pd.to_datetime(df['day'])
        df['open'] = df['open'].astype(float)
        df['close'] = df['close'].astype(float)
        df['high'] = df['close'].astype(float)
        df['low'] = df['close'].astype(float)
        df['volume'] = df['volume'].astype(int)
        df = df.reindex()
        self.log.debug('获取股票{} {}分钟数据, count={}'.format(code, period, df.shape[0]))

        return df

    @BaseFetch.retry_client
    def fetch_stock_north_south_flow(self, start: datetime = None,
                                     end: datetime = None) -> Optional[pd.DataFrame]:
        """
        股票北上南下净流入
        :param code:
        :param start:
        :param end:
        :return:
        """
        if not self.is_trade(start, end):
            self.log.info('start={}, end={}, 非交易日不同步...'.format(start, end))
            return None

        self.log.debug('获取股票北向资金 沪股通...')
        df = ak.stock_em_hsgt_north_net_flow_in(indicator='沪股通')
        if df is None:
            self.log.error('获取股票北向资金 沪股通失败')
            return None

        df.rename(columns={'value': 'sh_north_value'}, inplace=True)

        self.log.debug('获取股票北向资金 深股通...')
        df2 = ak.stock_em_hsgt_north_net_flow_in(indicator='深股通')
        if df2 is None:
            self.log.error('获取股票北向资金 深股通失败')
            return None

        df2.rename(columns={'value': 'sz_north_value'}, inplace=True)

        df = df.merge(df2, how='left', left_on=['date'], right_on=['date'])

        self.log.debug('获取股票南向资金 沪股通...')
        df2 = ak.stock_em_hsgt_south_net_flow_in(indicator='沪股通')
        if df2 is None:
            self.log.error('获取股票北向资金 沪股通失败')
            return None

        df2.rename(columns={'value': 'sh_south_value'}, inplace=True)

        df = df.merge(df2, how='left', left_on=['date'], right_on=['date'])

        self.log.debug('获取股票南向资金 深股通...')
        df2 = ak.stock_em_hsgt_south_net_flow_in(indicator='深股通')
        if df2 is None:
            self.log.error('获取股票南向资金 深股通失败')
            return None

        df2.rename(columns={'value': 'sz_south_value'}, inplace=True)
        df = df.merge(df2, how='left', left_on=['date'], right_on=['date'])

        df['sh_north_value'] = df['sh_north_value'].apply(lambda x: float(x))
        df['sz_north_value'] = df['sz_north_value'].apply(lambda x: float(x))
        df['north_value'] = df['sh_north_value'] + df['sz_north_value']

        df['sh_south_value'] = df['sh_south_value'].apply(lambda x: float(x))
        df['sz_south_value'] = df['sz_south_value'].apply(lambda x: float(x))
        df['south_value'] = df['sh_south_value'] + df['sz_south_value']

        if start is not None:
            date_str = start.strftime('%Y-%m-%d')
            cond = 'date >= "{}"'.format(date_str)
            df = df.query(cond)
        if end is not None:
            date_str = end.strftime('%Y-%m-%d')
            cond = 'date <= "{}"'.format(date_str)
            df = df.query(cond)
        df['trade_date'] = pd.to_datetime(df['date'], format='%Y-%m-%d')
        df.drop(columns=['date'], inplace=True)
        df = df.reindex()
        self.log.debug('获取股票北向资金, count={}'.format(df.shape[0]))

        return df

    @BaseFetch.retry_client
    def fetch_stock_his_divend(self, codes: List[str] = None) -> Optional[pd.DataFrame]:
        """
        股票历史分红数据，不获取详细的
        :param codes:
        :return:
        """
        self.log.debug('获取股票历史分红数据...')
        df = ak.stock_history_dividend()
        if df is None:
            self.log.error('获取股票历史分红数据失败')
            return None

        df.rename(columns={'代码': 'code', '名称': 'name', '上市日期': 'listing_date', '累计股息(%)': 'divend_acc',
                           '年均股息(%)': 'divend_avg', '分红次数': 'divend_count', '融资总额(亿)': 'financed_total',
                           '融资次数': 'financed_count'}, inplace=True)
        df.drop(columns=['详细'], inplace=True)
        df['listing_date'] = pd.to_datetime(df['listing_date'], format='%Y-%m-%d')
        # '600', '601', '603', '688' - sh, '000', '300', '002' - sz
        df['code'] = df['code'].apply(lambda x: 'sh' + x if x.startswith('6') else 'sz' + x)
        if codes is not None:
            cond = 'code in ["{}"]'.format("\",\"".join(codes))
            df = df.query(cond)
        df = df.reindex()
        self.log.debug('获取股票历史分红数据, count={}'.format(df.shape[0]))

        return df

    @BaseFetch.retry_client
    def fetch_sw_index_info(self, codes: List[str] = None) -> Optional[pd.DataFrame]:
        """
        申万一级行业信息
        :return:
        """
        self.log.debug('获取申万一级行业名称...')
        df = ak.sw_index_spot()
        if df is None:
            self.log.error('获取申万一级行业名称失败')
            return None
        data = None
        for _, item in df.iterrows():
            code = item['指数代码']
            if codes is not None and code in codes:
                continue
            name = item['指数名称']

            self.log.debug('获取申万一级行业{}{}成分...'.format(name, code))
            df2 = self.fetch_sw_index_detail(code=code)
            if df2 is None:
                self.log.debug('获取申万一级行业{}{}失败'.format(name, code))
                return None
            df2['index_code'] = code
            df2['index_name'] = name
            data = df2 if data is None else data.append(df2)

        data = data.reindex()
        self.log.debug('获取申万一级行业数据, count={}'.format(data.shape[0]))
        return data

    @BaseFetch.retry_client
    def fetch_sw_index_detail(self, code: str) -> Optional[pd.DataFrame]:
        """
        申万一级行业信息成分
        :return:
        """
        self.log.debug('获取申万一级行业{}成分...'.format(code))
        df = ak.sw_index_cons(index_code=code)
        if df is None:
            self.log.error('获取申万一级行业{}成分...'.format(code))
            return None

        # '600', '601', '603', '688' - sh, '000', '300', '002' - sz
        df['stock_code'] = df['stock_code'].apply(lambda x: 'sh' + x if x.startswith('6') else 'sz' + x)
        self.log.debug('获取申万一级行业{}成分, count={}'.format(code, df.shape[0]))

        return df

    def fetch_stock_rt_quote(self, codes: List[str]) -> Optional[pd.DataFrame]:
        # self.log.debug('获取股票{}实时行情...'.format(codes))
        symbols = []
        for code in codes:
            symbols.append(self.sina2xueqiu(code))

        df, msg = stock.get_quote(','.join(symbols))
        if df is None:
            self.log.debug('获取股票{}实时行情失败'.format(codes))
            return None
        df['symbol'] = df['symbol'].apply(self.xueqiu2sina)
        df.rename(columns={'last': 'close', 'symbol': 'code', 'turnover_rate': 'turnover', 'time': 'day_time'}, inplace=True)
        df.drop(columns=['change', 'percent', 'market_capital', 'float_market_capital', 'is_trading'], inplace=True)
        df['open'] = df['open'].astype(float)
        df['close'] = df['close'].astype(float)
        df['high'] = df['close'].astype(float)
        df['low'] = df['close'].astype(float)
        df['last_close'] = df['last_close'].astype(float)
        df['volume'] = df['volume'].astype(int)
        df['amount'] = df['amount'].astype(float)
        # self.log.debug('获取股票{}实时行情实时行情, count={}'.format(codes, df.shape[0]))
        return df

    @BaseFetch.retry_client
    def fetch_stock_new(self, codes: List[str] = None) -> Optional[pd.DataFrame]:
        self.log.debug('获取次新股行情...')
        df = ak.stock_zh_a_new()
        if df is None:
            self.log.error('获取次新股行情 失败')
            return None
        df.drop(columns=['code'], inplace=True)
        df.rename(columns={'symbol': 'code'}, inplace=True)

        if codes is not None:
            cond = 'code in ["{}"]'.format("\",\"".join(codes))
            df = df.query(cond)

        df = df.reindex()
        self.log.debug('获取股票成功, count={}'.format(df.shape[0]))
        return df


if __name__ == '__main__':
    aks = MyFetch()

    now = datetime.now()
    now_pre = now - timedelta(days=1)

    # ['day_time', 'code', 'high', 'low', 'close', 'volume', 'amount','turnover']
    # df = aks.fetch_stock_rt_quote(codes=['sh601099', 'sz000001'])
    # print(df)
    # print(df.columns)

    # day_time open high low close volume code
    # df = aks.fetch_stock_minute(code='sh601099', period='5')
    # print(df)
    # print(df.columns)

    # df = aks.fetch_stock_info()
    # print(df)

    # df = ak.stock_zh_a_daily(symbol='sz159949', adjust='')
    # print(df)
    # df, msg = stock.get_daily('159949.SZ', start_date='2017-06-06', end_date='2018-06-07')
    # print(df)
    #
    # df = ak.stock_zh_a_daily(symbol='sz000001', adjust='qfq-factor')
    # print(df)

    # df = ak.stock_zh_a_daily(symbol='159949', adjust='qfq')
    # print(df)

    # df = ak.fund_etf_hist_sina(symbol='sz159949')
    # print(df)

    df = aks.fetch_fund_daily_xueqiu(code='159805',
                                     start=datetime(year=2020, month=11, day=23),
                                     end=datetime(year=2020, month=11, day=27))

    print(df)

    # df = aks.fetch_stock_daily_xueqiu(code='sz159949')
    # print(df)

    # df = aks.fetch_stock_daily(code='sh600350', start=datetime(year=2020, month=11, day=23), end=datetime.now())
    # print(df)

    # df = ak.stock_zh_a_daily(symbol='sz000001')
    # print(df)

    # df = aks.fetch_stock_info()
    # print(df)
    #
    # print(df)

    # df = aks.fetch_index_daily(code='sh000001',
    #                            start=datetime.strptime('2020-01-01', '%Y-%m-%d'),
    #                            end=datetime.strptime('2020-11-12', '%Y-%m-%d'))
    # print(df)

    # df = aks.fetch_stock_rt_quote(codes=['sh000001', 'sz000001', 'sh600688'])
    # print(df)

    # df = aks.fetch_stock_rt_minute('sh600688', '5m')
    # print(df)

    # df = aks.fetch_stock_daily(code='sh689009')
    # print(df)

    # df = aks.fetch_stock_his_divend()
    # print(df)

    # df = aks.fetch_sw_index_info()
    # print(df)

    # df = aks.fetch_index_daily(code='sh399006')
    # print(df)

    # df = aks.fetch_stock_minute(code='sh688008', period='15')
    # print(df)

    # stock_zh_a_spot_df = ak.stock_zh_a_spot()
    # print(stock_zh_a_spot_df)
    #
    # stock_zh_a_daily_hfq_df = ak.stock_zh_kcb_daily(symbol="sh688160")
    # print(stock_zh_a_daily_hfq_df)
    #
    # stock_zh_a_minute_df = ak.stock_zh_a_minute(symbol='sh600751', period='1', adjust="qfq")
    # print(stock_zh_a_minute_df)
    #
    # stock_zh_a_new_df = ak.stock_zh_a_new()
    # print(stock_zh_a_new_df)
    #
    # stock_df = ak.stock_zh_index_spot()
    # print(stock_df)
    #
    # stock_zh_index_daily_df = ak.stock_zh_index_daily(symbol="sz399552")
    # print(stock_zh_index_daily_df)
    #
    # stock_zh_index_daily_tx_df = ak.stock_zh_index_daily_tx(symbol="sh000919")
    # print(stock_zh_index_daily_tx_df)
    #
    # stock_zh_kcb_spot_df = ak.stock_zh_kcb_spot()
    # print(stock_zh_kcb_spot_df)
    #
    # stock_zh_kcb_daily_df = ak.stock_zh_kcb_daily(symbol="sh688399", adjust="hfq")
    # print(stock_zh_kcb_daily_df)
    #
    # stock_em_account_df = ak.stock_em_account()
    # print(stock_em_account_df)
    #
    # stock_history_dividend_df = ak.stock_history_dividend()
    # print(stock_history_dividend_df)
    #
    # stock_info_a_code_name_df = ak.stock_info_a_code_name()
    # print(stock_info_a_code_name_df)
