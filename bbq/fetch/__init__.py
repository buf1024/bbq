import pandas as pd

from .ak_trade_date import is_trade_date
from .my_akshare import MyAKShare
from typing import List

__ak = MyAKShare()

fetch_stock_info = __ak.fetch_stock_info
fetch_stock_daily = __ak.fetch_stock_daily
fetch_stock_index = __ak.fetch_stock_index
fetch_index_daily = __ak.fetch_index_daily
fetch_stock_north_south_flow = __ak.fetch_stock_north_south_flow
fetch_stock_his_divend = __ak.fetch_stock_his_divend
fetch_sw_index_info = __ak.fetch_sw_index_info


def fetch_index_info(codes: List[str] = None) -> pd.DataFrame:
    data = pd.DataFrame(dict(
        code=['sh000001', 'sz399001', 'sh399006', 'sz399005', 'sh000300', 'sh000688',
              'sz399673', 'sz399550', 'sz399678', '3sz99007', 'sz399008', ],
        name=['上证综指', '深证成指', '创业板指', '中小板指', '沪深300', '科创50',
              '创业板50', '央视50', '深次新股', '深证300', '中小300']))

    if codes is not None and data is not None:
        cond = 'code in ["{}"]'.format("\",\"".join(codes))
        data = data.query(cond)
    return data.reindex()


is_trade_date = is_trade_date
