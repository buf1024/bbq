from barbar.common import load_strategy
from os.path import dirname
from barbar.config import conf_dict
import pandas as pd

__file_path = dirname(__file__)

fetcher = load_strategy(__file_path, 'barbar.fetch', ('fetcher.py',))

tushare_pro = fetcher['TusharePro'](conf_dict['tushare_pro']['token'])
sina = fetcher['Sina']()
tdx = fetcher['Tdx']()

get_trade_cal = tushare_pro.get_trade_cal
get_code_list = tdx.get_code_list
get_bar = tdx.get_bar
get_adj_factor = tushare_pro.get_adj_factor
get_index_bar = tdx.get_index_bar
get_rt_quot = sina.get_rt_quot
get_block_list = tdx.get_block_list
get_xdxr_list = tdx.get_xdxr_list


def get_index_list() -> pd.DataFrame:
    return pd.DataFrame(dict(
        code=['000001.SH', '399001.SZ', '399006.SZ', '399005.SZ', '000300.SH', '000688.SH',
              '399673.SZ', '399550.SZ', '399678.SZ', '399007.SZ', '399008.SZ', ],
        name=['上证综指', '深证成指', '创业板指', '中小板指', '沪深300', '科创50',
              '创业板50', '央视50', '深次新股', '深证300', '中小300']))


def init():
    return tdx.init()
