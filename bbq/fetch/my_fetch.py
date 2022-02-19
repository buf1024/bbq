from datetime import datetime, timedelta
from typing import Optional, List

import pandas as pd
import xlrd
from opendatatools import stock

import bbq.fetch.hiakshare as hiak
from bbq.fetch.base_fetch import BaseFetch
from bbq.fetch.stock_eastmoney import StockEastmoney
from bbq.retry import retry
from bbq.fetch.my_trade_date import is_trade_date
import os

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
        self.eastmoney = StockEastmoney()

    @retry(name='MyFetch')
    def fetch_stock_listing_date(self, code: str) -> Optional[datetime]:
        """
        获取某只股票上市时间
        :param code: 股票代码 sh/sz开头
        :return: 上市时间
        """
        self.log.debug('获取股票上市日期, code={}...'.format(code))
        if code.lower()[:2] == 'sh':
            df = hiak.stock_info_sh_name_code(indicator="主板A股")
            if df is not None and not df.empty:
                df = df[df['公司代码'] == code.lower()[2:]]
                if len(df) == 0:
                    self.log.info('股票代码错误/已退市, code={}...'.format(code))
                    return None
                list_date = df.iloc[0]['上市日期']
                list_date = datetime(year=list_date.year, month=list_date.month, day=list_date.day)

                self.log.info('股票{}上市日期{}'.format(code, list_date))
                return list_date

        if code.lower()[:2] == 'sz':
            df = hiak.stock_info_sz_name_code(indicator="A股列表")
            if df is not None and not df.empty:
                df = df[df['A股代码'] == code.lower()[2:]]
                if len(df) == 0:
                    self.log.info('股票代码错误, code={}...'.format(code))
                    return None
                list_date = datetime.strptime(df.iloc[0]['A股上市日期'], '%Y-%m-%d')
                self.log.info('股票{}上市日期{}'.format(code, list_date))
                return list_date
        return None

    @retry(name='MyFetch')
    def fetch_stock_info(self, codes: List[str] = None) -> Optional[pd.DataFrame]:
        """
        股票信息
        :param codes: 需要获取股票代码, None为全部
        :return: code(股票代码)  name(股票名称) listing_date(股票上市日期) block(板块)
        """
        data, market = None, 'shsz'
        if codes is not None:
            market = ''
            if any(filter(lambda x: x[:2] == 'sh', codes)):
                market = market + 'sh'
            if any(filter(lambda x: x[:2] == 'sz', codes)):
                market = market + 'sz'
        if 'sh' in market:
            self.log.debug('获取上证股票...')
            df = hiak.stock_info_sh_name_code(indicator="主板A股")
            if df is None:
                self.log.debug('上证 主板A股 失败')
                return None
            df['block'] = '主板'

            df2 = hiak.stock_info_sh_name_code(indicator="科创板")
            if df2 is None:
                self.log.error('上证 科创板 失败')
                return None
            df2['block'] = '科创板'
            df = df.append(df2)

            df['code'] = df['公司代码'].apply(lambda x: 'sh' + x)
            df['listing_date'] = pd.to_datetime(df['上市日期'], format='%Y-%m-%d')
            data = df.rename(columns={'公司简称': 'name'})[['code', 'name', 'listing_date', 'block']]

        if 'sz' in market:
            self.log.debug('获取深证股票...')
            df = hiak.stock_info_sz_name_code(indicator="主板")
            if df is None:
                self.log.error('深证 主板 失败')
                return None
            df['block'] = '主板'
            df.dropna(subset=['A股代码'], inplace=True)
            df.dropna(subset=['A股简称'], inplace=True)
            df = df[df['A股代码'] != '000nan']
            df['A股代码'] = df['公司代码'].astype(str).str.zfill(6)

            df2 = hiak.stock_info_sz_name_code(indicator="中小企业板")
            if df2 is None:
                self.log.error('深证 中小企业板 失败')
                return None
            df2['block'] = '中小板'

            df = df.append(df2)

            df2 = hiak.stock_info_sz_name_code(indicator="创业板")
            if df2 is None:
                self.log.error('深证 创业板 失败')
                return None
            df2['block'] = '创业板'
            df = df.append(df2)

            df['code'] = df['A股代码'].apply(lambda x: 'sz' + x)
            df['listing_date'] = pd.to_datetime(df['A股上市日期'], format='%Y-%m-%d')
            data2 = df.rename(columns={'公司简称': 'name'})[['code', 'name', 'listing_date', 'block']]
            data = data.append(data2) if data is not None else data2
            data.drop_duplicates(['code'], inplace=True)

        if codes is not None and data is not None:
            cond = 'code in ["{}"]'.format("\",\"".join(codes))
            data = data.query(cond)

        self.log.info('获取东方财富融资融券标的')
        margin_info = self.eastmoney.get_stock_margin_code()
        data = data.merge(margin_info, how='left', on='code')
        data['is_margin'].fillna(value=0, inplace=True)

        data = data.reset_index(drop=True)
        self.log.debug('获取股票成功, count={}'.format(self.df_size(data)))
        return data

    @retry(name='MyFetch')
    def fetch_stock_adj_factor(self, code: str, start: datetime = None, end: datetime = None) -> Optional[pd.DataFrame]:
        """
        获取股票复权因子

        :param end:
        :param start:
        :param code: 股票代码
        :return: trade_date hfq_factor(后复权因子) qfq_factor code(前复权因子)
        """
        try:
            self.log.debug('获取股票{}后复权因子...'.format(code))
            df_hfq_factor = hiak.stock_zh_a_daily(symbol=code, adjust='hfq-factor')
            if df_hfq_factor is None:
                self.log.error('获取股票{}后复权因子失败'.format(code))
                return None
            df_hfq_factor.dropna(inplace=True)

            self.log.debug('获取股票{}前复权因子...'.format(code))
            df_qfq_factor = hiak.stock_zh_a_daily(symbol=code, adjust='qfq-factor')
            if df_qfq_factor is None:
                self.log.error('获取股票{}前复权因子失败'.format(code))
                return None
            df_qfq_factor.dropna(inplace=True)
            df = df_hfq_factor.merge(df_qfq_factor, how='left', left_on=['date'], right_on=['date'])
            df = df.reset_index(drop=True)
            df['code'] = code
            df.rename(columns={'date': 'trade_date'}, inplace=True)
            if not df.empty:
                now = datetime.now()
                now = datetime(year=now.year, month=now.month, day=now.day)
                df['sync_date'] = now

            if start is not None:
                date_str = start.strftime('%Y-%m-%d')
                cond = 'trade_date >= "{}"'.format(date_str)
                df = df.query(cond)
            if end is not None:
                date_str = end.strftime('%Y-%m-%d')
                cond = 'trade_date <= "{}"'.format(date_str)
                df = df.query(cond)
            df = df.reset_index(drop=True)
            self.log.debug('获取股票{}复权数据成功, count={}'.format(code, self.df_size(df)))
            return df
        except Exception as e:
            self.log.info('获取股票{}复权数据失败(可能未曾复权)'.format(code))

        return None

    @retry(name='MyFetch')
    def fetch_stock_daily_xueqiu(self, code: str, start: datetime = None, end: datetime = None) -> Optional[
        pd.DataFrame]:
        """
        从雪球获取股票日线数据

        :param code: 股票代码
        :param start: 开始时间
        :param end: 结束时间
        :return:  volume     open     high      low    close  turnover(换手率)       date
        """
        if not self.is_trade(start, end):
            self.log.info('code={}, start={}, end={}, 非交易日不同步...'.format(code, start, end))
            return None
        self.log.debug('获取雪球股票{}日线数据, start={}, end={}...'.format(code, start, end))
        if start is None:
            start = self.fetch_stock_listing_date(code=code)
            if start is None:
                return None
        if end is None:
            now = datetime.now()
            end = now
            if now < datetime(year=now.year, month=now.month, day=now.day, hour=15, minute=30):
                end = now - timedelta(days=1)

        start_date, end_date = start.strftime('%Y-%m-%d'), end.strftime('%Y-%m-%d')
        df, msg = stock.get_daily(symbol=self.sina2xueqiu(code), start_date=start_date, end_date=end_date)
        if df is not None and not df.empty:
            df.dropna(inplace=True)
            df.rename(columns={'last': 'close', 'turnover_rate': 'turnover'}, inplace=True)
            df['turnover'] = df['turnover']
            df['date'] = pd.to_datetime(df['time'], format='%Y-%m-%d')
            df.drop(columns=['symbol', 'change', 'percent', 'time'], inplace=True)
        else:
            self.log.warn('获取股票{}未复权数据失败(可能停牌或未上市): {}'.format(code, msg))
            return None
        self.log.debug('获取雪球股票{}日线数据, count={}'.format(code, self.df_size(df)))
        return df

    @retry(name='MyFetch')
    def fetch_stock_daily(self, code: str, start: datetime = None, end: datetime = None, adjust: bool = True) -> \
            Optional[pd.DataFrame]:
        """
        股票日线数据, 组合新浪和雪球两种方式，如果需要复权，复权方式采用后复权方式。

        :param adjust: 是否复权
        :param code: 股票代码
        :param start: 开始时间
        :param end: 结束时间
        :return: volume, open, high, low, close, turnover(换手率), trade_date,
                hfq_factor(后复权因子), code
        """
        if not self.is_trade(start, end):
            self.log.debug('code={}, start={}, end={}, 非交易日不同步...'.format(code, start, end))
            return None

        df_hfq_factor = None
        if adjust:
            try:
                self.log.debug('获取股票{}后复权因子...'.format(code))
                df_hfq_factor = hiak.stock_zh_a_daily(symbol=code, adjust='hfq-factor')
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
                df = hiak.stock_zh_a_daily(symbol=code)
                if df is not None and not df.empty:
                    df.dropna(inplace=True)
                    df.drop(columns=['outstanding_share'], inplace=True)
                    df['turnover'] = df['turnover'].apply(lambda x: round(x * 100, 2))
                    df.reset_index(drop=True, inplace=True)
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

        df = df.reset_index(drop=True)
        df.rename(columns={'date': 'trade_date'}, inplace=True)
        self.log.debug('获取股票{}日线数据, count={}'.format(code, self.df_size(df)))

        return df

    @retry(name='MyFetch')
    def fetch_stock_index(self, code: str, start: datetime = None, end: datetime = None) -> Optional[pd.DataFrame]:
        """
        股票指标数据
        :param code: 股票代码
        :param start: 开始时间
        :param end: 结束时间
        :return: trade_date, pe(市盈率), pe_ttm(市盈率TTM), pb(市净率), ps(市销率),
                ps_ttm(市销率TTM), dv_ratio(股息率), dv_ttm(股息率TTM), total_mv(总市值), code
        """
        if not self.is_trade(start, end):
            self.log.info('code={}, start={}, end={}, 非交易日不同步...'.format(code, start, end))
            return None

        self.log.debug('获取股票{}指标数据...'.format(code))
        code_ak = code
        if code.startswith('sh') or code.startswith('sz'):
            code_ak = code[2:]
        df = hiak.stock_a_lg_indicator(stock=code_ak)
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

        df = df.reset_index(drop=True)

        self.log.debug('获取股票{}指标数据, count={}'.format(code, self.df_size(df)))
        return df

    @retry(name='MyFetch')
    def fetch_stock_index_daily(self, code: str, start: datetime = None, end: datetime = None) -> Optional[
        pd.DataFrame]:
        """
        指数日线数据
        :param code: 指数代码
        :param start: 开始时间
        :param end: 结束时间
        :return: volume, open, high, low, close, trade_date, code
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
                if df is not None and not df.empty:
                    df.rename(columns={'last': 'close'}, inplace=True)
                    df['date'] = pd.to_datetime(df['time'], format='%Y-%m-%d')
                    df.drop(columns=['symbol', 'change', 'percent', 'time', 'turnover_rate'], inplace=True)
                else:
                    self.log.error('获取指数{}日线数据失败: {}'.format(code, msg))
        if df is None:
            df = hiak.stock_zh_index_daily(symbol=code)
            if df is not None and not df.empty:
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
        self.log.debug('获取指数{}日线数据, count={}'.format(code, self.df_size(df)))
        return df

    @retry(name='MyFetch')
    def fetch_stock_minute(self, code: str, period: str, adjust: str = '', start: datetime = None,
                           end: datetime = None) -> Optional[pd.DataFrame]:
        """
        股票分钟数据

        :param code: 股票代码
        :param period: 1, 5, 15, 30, 60 分钟的数据
        :param adjust: 默认为空: 返回不复权的数据; qfq: 返回前复权后的数据; hfq: 返回后复权后的数据;
        :param start: 开始时间
        :param end: 结束时间
        :return: day_time open high low close volume code
        """
        if not self.is_trade(start, end):
            self.log.info('code={}, start={}, end={}, 非交易日不同步...'.format(code, start, end))
            return None
        self.log.debug('获取股票{} {}分钟数据...'.format(code, period))
        df = hiak.stock_zh_a_minute(symbol=code, period=period, adjust=adjust)
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
        df['high'] = df['high'].astype(float)
        df['low'] = df['low'].astype(float)
        df['volume'] = df['volume'].astype(int)
        df.drop(columns=['day'], inplace=True)
        df = df.reset_index(drop=True)
        self.log.debug('获取股票{} {}分钟数据, count={}'.format(code, period, self.df_size(df)))

        return df

    @retry(name='MyFetch')
    def fetch_stock_north_south_flow(self, start: datetime = None,
                                     end: datetime = None) -> Optional[pd.DataFrame]:
        """
        股票北上南下净流入
        :param start: 开始时间
        :param end: 结束时间
        :return: sh_north_value(沪股通北向净流入(单位: 亿元)), sz_north_value(深股通北向净流入(单位: 亿元)),
                 sh_south_value(沪股通南向净流入(单位: 亿元)), sz_south_value(深股通南向净流入(单位: 亿元)),
                 north_value(北向净流入(单位: 亿元)), south_value(南向净流入(单位: 亿元)), 'trade_date'
        """
        if not self.is_trade(start, end):
            self.log.info('start={}, end={}, 非交易日不同步...'.format(start, end))
            return None

        self.log.debug('获取股票北向资金 沪股通...')
        df = hiak.stock_em_hsgt_north_net_flow_in(indicator='沪股通')
        if df is None:
            self.log.error('获取股票北向资金 沪股通失败')
            return None

        df.rename(columns={'value': 'sh_north_value'}, inplace=True)

        self.log.debug('获取股票北向资金 深股通...')
        df2 = hiak.stock_em_hsgt_north_net_flow_in(indicator='深股通')
        if df2 is None:
            self.log.error('获取股票北向资金 深股通失败')
            return None

        df2.rename(columns={'value': 'sz_north_value'}, inplace=True)

        df = df.merge(df2, how='left', left_on=['date'], right_on=['date'])

        self.log.debug('获取股票南向资金 沪股通...')
        df2 = hiak.stock_em_hsgt_south_net_flow_in(indicator='沪股通')
        if df2 is None:
            self.log.error('获取股票北向资金 沪股通失败')
            return None

        df2.rename(columns={'value': 'sh_south_value'}, inplace=True)

        df = df.merge(df2, how='left', left_on=['date'], right_on=['date'])

        self.log.debug('获取股票南向资金 深股通...')
        df2 = hiak.stock_em_hsgt_south_net_flow_in(indicator='深股通')
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
        df = df.reset_index(drop=True)
        self.log.debug('获取股票北向资金, count={}'.format(self.df_size(df)))

        return df

    @retry(name='MyFetch')
    def fetch_stock_his_divend(self, codes: List[str] = None) -> Optional[pd.DataFrame]:
        """
        股票历史分红数据，不获取详细的
        :param codes: 股票列表
        :return: code, name, listing_date(上市日期), divend_acc(累计股息(%)), divend_avg(年均股息(%)),
                divend_count(分红次数), financed_total(融资总额(亿)), financed_count(融资次数)
        """
        self.log.debug('获取股票历史分红数据...')
        df = hiak.stock_history_dividend()
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
        if not df.empty:
            now = datetime.now()
            now = datetime(year=now.year, month=now.month, day=now.day)
            df['sync_date'] = now

        if codes is not None:
            cond = 'code in ["{}"]'.format("\",\"".join(codes))
            df = df.query(cond)
        df = df.reset_index(drop=True)
        self.log.debug('获取股票历史分红数据, count={}'.format(self.df_size(df)))

        return df

    @retry(name='MyFetch')
    def fetch_stock_sw_index_info(self, codes: List[str] = None) -> Optional[pd.DataFrame]:
        """
        申万一级行业信息

        :param codes 行业代码列表
        :return: stock_code(股票代码) stock_name start_date(开始日期)  weight(权重) index_code(行业代码) index_name(行业名称)
        """
        self.log.debug('获取申万一级行业名称...')
        df = hiak.sw_index_spot()
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
            df2 = self.fetch_stock_sw_index_detail(code=code)
            if df2 is None:
                self.log.debug('获取申万一级行业{}{}失败'.format(name, code))
                return None
            df2['index_code'] = code
            df2['index_name'] = name
            data = df2 if data is None else data.append(df2)

        data = data.reset_index(drop=True)
        self.log.debug('获取申万一级行业数据, count={}'.format(self.df_size(data)))
        return data

    @retry(name='MyFetch')
    def fetch_stock_sw_index_detail(self, code: str) -> Optional[pd.DataFrame]:
        """
        申万一级行业信息成分
        :param code 行业代码
        :return: stock_code stock_name start_date  weight
        """
        self.log.debug('获取申万一级行业{}成分...'.format(code))
        df = hiak.sw_index_cons(index_code=code)
        if df is None:
            self.log.error('获取申万一级行业{}成分...'.format(code))
            return None

        # '600', '601', '603', '688' - sh, '000', '300', '002' - sz
        df['stock_code'] = df['stock_code'].apply(lambda x: 'sh' + x if x.startswith('6') else 'sz' + x)
        self.log.debug('获取申万一级行业{}成分, count={}'.format(code, self.df_size(df)))

        return df

    def fetch_stock_rt_quote(self, codes: List[str]) -> Optional[pd.DataFrame]:
        """
        获取实时行情

        :param codes: 股票代码
        :return:  day_time, code, last_close(昨收价), open, high, low, close, volume, amount, turnover
        """
        # self.log.debug('获取股票{}实时行情...'.format(codes))
        symbols = []
        for code in codes:
            symbols.append(self.sina2xueqiu(code))

        df, msg = stock.get_quote(','.join(symbols))
        if df is None:
            self.log.debug('获取股票{}实时行情失败'.format(codes))
            return None
        df['symbol'] = df['symbol'].apply(self.xueqiu2sina)
        df.rename(columns={'last': 'close', 'symbol': 'code', 'turnover_rate': 'turnover', 'time': 'day_time'},
                  inplace=True)
        df.drop(columns=['change', 'percent', 'market_capital', 'float_market_capital', 'is_trading'], inplace=True)
        df['open'] = df['open'].astype(float)
        df['close'] = df['close'].astype(float)
        df['high'] = df['high'].astype(float)
        df['low'] = df['low'].astype(float)
        df['last_close'] = df['last_close'].astype(float)
        df['volume'] = df['volume'].astype(int)
        df['amount'] = df['amount'].astype(float)
        # self.log.debug('获取股票{}实时行情实时行情, count={}'.format(codes, df.shape[0]))
        return df

    @retry(name='MyFetch')
    def fetch_stock_new_quote(self, codes: List[str] = None) -> Optional[pd.DataFrame]:
        """
        取次新股行情
        :param codes: 次新股股票代码
        :return: code, name, open, high, low, volume, amount, turnover
        """
        self.log.debug('获取次新股行情...')
        df = hiak.stock_zh_a_new()
        if df is None:
            self.log.error('获取次新股行情 失败')
            return None
        df.drop(columns=['code', 'mktcap'], inplace=True)
        df.rename(columns={'symbol': 'code', 'turnoverratio': 'turnover'}, inplace=True)

        if codes is not None:
            cond = 'code in ["{}"]'.format("\",\"".join(codes))
            df = df.query(cond)

        df = df.reset_index(drop=True)
        self.log.debug('获取股票成功, count={}'.format(self.df_size(df)))
        return df

    @retry(name='MyFetch')
    def fetch_fund_daily_xueqiu(self, code: str,
                                start: datetime = datetime(year=1990, month=1, day=1),
                                end: datetime = None) -> Optional[pd.DataFrame]:
        """
        从雪球获取场内基金日k线数据

        :param code: 基金代码
        :param start: 开始时间
        :param end: 结束时间
        :return: volume open high low close turnover trade_date code
        """
        if not self.is_trade(start, end):
            self.log.info('code={}, start={}, end={}, 非交易日不同步...'.format(code, start, end))
            return None
        self.log.debug('获取雪球场内基金{}日线数据, start={}, end={}...'.format(code, start, end))
        if start is None:
            start = datetime(year=1990, month=1, day=1)

        if end is None:
            now = datetime.now()
            end = now
            if now < datetime(year=now.year, month=now.month, day=now.day, hour=15, minute=30):
                end = now - timedelta(days=1)

        start_date, end_date = start.strftime('%Y-%m-%d'), end.strftime('%Y-%m-%d')
        df, msg = stock.get_daily(symbol=self.fund2xueqiu(code), start_date=start_date, end_date=end_date)
        if df is not None and not df.empty:
            df.dropna(inplace=True)
            df.rename(columns={'last': 'close', 'turnover_rate': 'turnover'}, inplace=True)
            df['turnover'] = df['turnover']
            df['trade_date'] = pd.to_datetime(df['time'], format='%Y-%m-%d')
            df.drop(columns=['symbol', 'change', 'percent', 'time'], inplace=True)
            df['code'] = code
        else:
            self.log.warn('获取基金{}日线数据失败(可能未上市): {}'.format(code, msg))
            return None
        self.log.debug('获取雪球场内基金{}日线数据, count={}'.format(code, self.df_size(df)))
        return df

    @retry(name='MyFetch')
    def fetch_stock_margin(self, code: str, start: datetime = None, end: datetime = None) -> Optional[pd.DataFrame]:
        """
        获取股票融资融券信息

        :return
        股票代码(CODE) 股票名称(NAME)
        交易日期(DATE)	收盘价(元)(SPJ) 涨跌幅(%)(ZDF)
        融资: 余额(元)(RZYE)	余额占流通市值比(%)(RZYEZB)	买入额(元)(RZMRE)	偿还额(元)(RZCHE)	净买入(元)(RZJME)
        融券: 余额(元)(RQYE)	余量(股)(RQYL)	卖出量(股)(RQMCL)	偿还量(股)(RQCHL)	净卖出(股)(RQJMG)
        融资融券余额(元)(RZRQYE)	融资融券余额差值(元)(RZRQYECZ)
        """
        self.log.debug('获取东方财富{}融资融券数据, start={}, end={}...'.format(code, start, end))
        df = self.eastmoney.get_stock_margin(code=code, start=start, end=end)
        if not df.empty:
            df['sync_date'] = datetime.now()
        self.log.debug('获取东方财富{}融资融券数据, count={}'.format(code, self.df_size(df)))
        return df

    @retry(name='MyFetch')
    def fetch_stock_concept(self, start=None) -> Optional[pd.DataFrame]:
        q_items = []
        # 概念
        df = hiak.stock_board_concept_name_ths()
        if df is None or df.empty:
            return None
        df['日期'] = df['日期'].apply(lambda x: datetime(year=x.year, month=x.month, day=x.day))
        if start is not None:
            df = df[df['日期'] >= start]

        for item in df.to_dict('records'):
            name = item['概念名称']
            code_df = hiak.stock_board_concept_cons_ths(symbol=name)
            if code_df is None or code_df.empty:
                continue
            concept_code = item['代码'].split('/')[6]
            for code_item in code_df.to_dict('records'):
                code, stock_name = code_item['代码'], code_item['名称']
                code = 'sh' + code if code.startswith('6') else 'sz' + code
                q_items.append(dict(concept_code=concept_code, concept_date=item['日期'], concept_name=name,
                                    stock_code=code, stock_name=stock_name))
        return pd.DataFrame(q_items)

    @retry(name='MyFetch')
    def fetch_fund_info(self, codes: List[str] = None, types: List[str] = None) -> Optional[pd.DataFrame]:
        """
        获取天天基金基本信息

        :param codes:  基金代码
        :param types:  基金类型，可选为：
        "ETF-场内", "QDII", "QDII-ETF", "QDII-指数", "债券型", "债券指数", "分级杠杆",
        "固定收益", "定开债券", "混合-FOF", "混合型", "理财型", "联接基金", "股票型", "股票指数", "货币型"
        :return: code  name  type
        """
        self.log.debug('获取天天基金基本信息, codes={}, types={}...'.format(codes, types))
        df = hiak.fund_em_fund_name()
        if df is not None and not df.empty:
            df.dropna(inplace=True)
            df.drop(columns=['拼音缩写', '拼音全称'], inplace=True)
            df.rename(columns={'基金代码': 'code', '基金简称': 'name', '基金类型': 'type'}, inplace=True)

        if codes is not None and df is not None:
            cond = 'code in ["{}"]'.format("\",\"".join(codes))
            df = df.query(cond)

        if types is not None and df is not None:
            cond = 'type in ["{}"]'.format("\",\"".join(types))
            df1 = df.query(cond)

            df2 = pd.DataFrame()
            for typ in types:
                if 'ETF' in typ:
                    df2 = df[df['name'].str.contains('ETF') &
                             ~df['name'].str.contains('联接') &
                             ~df['type'].str.contains('货币型')]

            if df1 is None or df1.empty:
                df = df2
            else:
                df1.append(df2)

        df = df.reset_index(drop=True)
        self.log.debug('获取天天基金基本信息, count={}'.format(self.df_size(df)))
        return df

    @retry(name='MyFetch')
    def fetch_fund_net(self, code: str, start: datetime = None, end: datetime = None, ) -> Optional[pd.DataFrame]:
        """
        获取天天基金净值信息

        :param code: 基金代码
        :param start: 开始时间
        :param end: 结束时间
        :return:
            代码(code) 净值日期(trade_date) 单位净值(net) 累计净值(net_acc) 日增长率(rise)
            申购状态(apply_status) 赎回状态(redeem_status)
        """
        self.log.debug('获取天天基金基本信息, code={}, start={} end={}...'.format(code, start, end))
        start_date = start.strftime('%Y%m%d') if start is not None else '19900101'
        end_date = end.strftime('%Y%m%d') if end is not None else datetime.now().strftime('%Y%m%d')
        df = hiak.fund_em_etf_fund_info(code, start_date, end_date)
        if df is not None and not df.empty:
            df.dropna(inplace=True)
            df.rename(columns={'净值日期': 'trade_date', '单位净值': 'net',
                               '累计净值': 'net_acc', '日增长率': 'rise',
                               '申购状态': 'apply_status', '赎回状态': 'redeem_status'}, inplace=True)
            df['code'] = code
            df['trade_date'] = pd.to_datetime(df['trade_date'], format='%Y-%m-%d')
            # df['rise'] = df['rise'].apply(lambda x: 0.0 if len(x) == 0 else float(x))
            try:
                df.astype({'net': 'float', 'net_acc': 'float'})
            except:
                def _cvt(s):
                    s['net'] = float(s['net'])
                    if len(s['net_acc']) == 0:
                        s['net_acc'] = s['net']
                    return s

                df = df.apply(func=_cvt, axis=1)
            df = df.reset_index(drop=True)
        self.log.debug('获取天天基金净值信息, count={}'.format(self.df_size(df)))
        return df


if __name__ == '__main__':
    aks = MyFetch()

    # tdf = aks.fetch_stock_concept(start=datetime(year=2022, month=1, day=5))
    # print(tdf)

    # tdf = aks.fetch_stock_listing_date(code='sz000001')
    # print(tdf)

    # tdf = aks.fetch_stock_info()
    # print(tdf)

    # tdf = aks.fetch_stock_adj_factor(code='sh600027')
    # print(tdf)

    # tdf = aks.fetch_stock_daily_xueqiu(code='sh600063', start=None, end=datetime(year=2021, month=6, day=2))
    # print(tdf)

    # tdf = aks.fetch_stock_margin(code='sz000001',
    #                              start=datetime.strptime('2021-11-01', '%Y-%m-%d'),
    #                              end=datetime.strptime('2021-12-03', '%Y-%m-%d'))
    #
    # print(tdf)

    tdf = aks.fetch_stock_rt_quote(codes=['sh601099', 'sz000001'])
    print(tdf)

    # day_time open high low close volume code
    # tdf = aks.fetch_stock_minute(code='sh601099', period='5')
    # print(tdf)

    #
    # tdf = aks.fetch_stock_daily(code='sh600350', start=datetime(year=2020, month=11, day=23), end=datetime.now())
    # print(tdf)

    # tdf = aks.fetch_stock_index(code='sz002847',
    #                             start=datetime(year=2022, month=1, day=5),
    #                             end=datetime(year=2022, month=1, day=5))
    # print(tdf)

    # tdf = hiak.stock_zh_a_daily(symbol='sz000001')
    # print(tdf)

    # tdf = aks.fetch_stock_index_daily(code='sh000001',
    #                                   start=datetime.strptime('2020-01-01', '%Y-%m-%d'),
    #                                   end=datetime.strptime('2020-11-12', '%Y-%m-%d'))
    # print(tdf)

    # tdf = aks.fetch_stock_rt_quote(codes=['sh000001', 'sz000001', 'sh600688'])
    # print(tdf)

    # tdf = aks.fetch_stock_rt_minute('sh600688', '5m')
    # print(tdf)

    # tdf = aks.fetch_stock_daily(code='sh689009')
    # print(tdf)

    # tdf = aks.fetch_stock_his_divend()
    # print(tdf)

    # tdf = aks.fetch_stock_sw_index_info()
    # print(tdf)

    # tdf = aks.fetch_stock_sw_index_detail(code='801010')
    # print(tdf)

    # tdf = aks.fetch_stock_new_quote()
    # print(tdf)

    # tdf = aks.fetch_stock_north_south_flow()
    # print(tdf)

    # tdf = aks.fetch_stock_index_daily(code='sh399006')
    # print(tdf)

    # tdf = aks.fetch_stock_minute(code='sh688008', period='15')
    # print(tdf)

    # stock_zh_a_spot_df = hiak.stock_zh_a_spot()
    # print(stock_zh_a_spot_df)
    #
    # stock_zh_a_daily_hfq_df = hiak.stock_zh_kcb_daily(symbol="sh688160")
    # print(stock_zh_a_daily_hfq_df)
    #
    # stock_zh_a_minute_df = hiak.stock_zh_a_minute(symbol='sh600751', period='1', adjust="qfq")
    # print(stock_zh_a_minute_df)
    #
    # stock_zh_a_new_df = hiak.stock_zh_a_new()
    # print(stock_zh_a_new_df)
    #
    # stock_df = hiak.stock_zh_index_spot()
    # print(stock_df)
    #
    # stock_zh_index_daily_df = hiak.stock_zh_index_daily(symbol="sz399552")
    # print(stock_zh_index_daily_df)
    #
    # stock_zh_index_daily_tx_df = hiak.stock_zh_index_daily_tx(symbol="sh000919")
    # print(stock_zh_index_daily_tx_df)
    #
    # stock_zh_kcb_spot_df = hiak.stock_zh_kcb_spot()
    # print(stock_zh_kcb_spot_df)
    #
    # stock_zh_kcb_daily_df = hiak.stock_zh_kcb_daily(symbol="sh688399", adjust="hfq")
    # print(stock_zh_kcb_daily_df)
    #
    # stock_em_account_df = hiak.stock_em_account()
    # print(stock_em_account_df)
    #
    # stock_history_dividend_df = hiak.stock_history_dividend()
    # print(stock_history_dividend_df)
    #
    # stock_info_a_code_name_df = hiak.stock_info_a_code_name()
    # print(stock_info_a_code_name_df)

    # tdf = aks.fetch_fund_info(types=['ETF-场内'])
    # print(tdf)

    # tdf = aks.fetch_fund_daily_xueqiu(code='159793',
    #                                   # start=datetime(year=1900, month=11, day=23),
    #                                   end=datetime(year=2021, month=12, day=12))
    #
    # print(tdf)

    # tdf = aks.fetch_fund_net(code='159608',
    #                          start=datetime.strptime('1990-01-01', '%Y-%m-%d'),
    #                          end=datetime.strptime('2021-12-11', '%Y-%m-%d'))
    # print(tdf)
