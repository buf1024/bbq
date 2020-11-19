import pandas as pd

from .my_trade_date import is_trade_date
from .my_fetch import MyFetch
from typing import List

__my_fetch = MyFetch()

fetch_stock_info = __my_fetch.fetch_stock_info
fetch_stock_daily = __my_fetch.fetch_stock_daily
fetch_stock_index = __my_fetch.fetch_stock_index
fetch_index_daily = __my_fetch.fetch_index_daily
fetch_stock_north_south_flow = __my_fetch.fetch_stock_north_south_flow
fetch_stock_his_divend = __my_fetch.fetch_stock_his_divend
fetch_sw_index_info = __my_fetch.fetch_sw_index_info
fetch_stock_rt_quote = __my_fetch.fetch_stock_rt_quote
fetch_stock_new = __my_fetch.fetch_stock_new
fetch_stock_adj_factor = __my_fetch.fetch_stock_adj_factor


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
