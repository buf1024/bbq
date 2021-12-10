import pandas as pd

from .my_trade_date import is_trade_date
from .my_fetch import MyFetch
from .fund_eastmoney import FundEastmoney
from .sina import Sina
from typing import List

my_fetch = MyFetch()
eastmoney_fetch = FundEastmoney()
sina_fetch = Sina()

fetch_stock_listing_date = my_fetch.fetch_stock_listing_date
fetch_stock_info = my_fetch.fetch_stock_info
fetch_stock_daily = my_fetch.fetch_stock_daily
fetch_stock_index = my_fetch.fetch_stock_index
fetch_stock_index_daily = my_fetch.fetch_stock_index_daily
fetch_stock_north_south_flow = my_fetch.fetch_stock_north_south_flow
fetch_stock_his_divend = my_fetch.fetch_stock_his_divend
fetch_stock_sw_index_info = my_fetch.fetch_stock_sw_index_info
fetch_stock_rt_quote = my_fetch.fetch_stock_rt_quote
fetch_stock_new_quote = my_fetch.fetch_stock_new_quote
fetch_stock_adj_factor = my_fetch.fetch_stock_adj_factor
fetch_stock_minute = my_fetch.fetch_stock_minute
fetch_stock_margin = my_fetch.fetch_stock_margin

fetch_fund_net = my_fetch.fetch_fund_net
fetch_fund_info = my_fetch.fetch_fund_info
fetch_fund_daily = my_fetch.fetch_fund_daily_xueqiu


def fetch_index_info(codes: List[str] = None) -> pd.DataFrame:
    data = pd.DataFrame(dict(
        code=['sh000001', 'sz399001', 'sz399006', 'sz399102', 'sz399005', 'sh000300', 'sh000688',
              'sz399673', 'sz399550', 'sz399678', 'sz399007', 'sz399008', ],
        name=['上证综指', '深证成指', '创业板指', '创业板综指', '中小板指', '沪深300', '科创50',
              '创业板50', '央视50', '深次新股', '深证300', '中小300']))

    if codes is not None and data is not None:
        cond = 'code in ["{}"]'.format("\",\"".join(codes))
        data = data.query(cond)
    return data.reindex()


is_trade_date = is_trade_date

__all__ = [
    'fetch_stock_listing_date',
    'fetch_stock_info',
    'fetch_stock_daily',
    'fetch_stock_index',
    'fetch_stock_index_daily',
    'fetch_stock_north_south_flow',
    'fetch_stock_his_divend',
    'fetch_stock_sw_index_info',
    'fetch_stock_rt_quote',
    'fetch_stock_new_quote',
    'fetch_stock_adj_factor',
    'fetch_stock_minute',
    'fetch_fund_net',
    'fetch_fund_info',
    'fetch_fund_daily',
    'fetch_index_info',
    'is_trade_date'
]
