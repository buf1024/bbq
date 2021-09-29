"""
一个乱来的股票投机分析工具
"""

__version__ = "0.0.1"
__author__ = "450171094@qq.com"

from .log import *
from .common import *
from .retry import *

from bbq.config import *
from bbq.data import *
from bbq.fetch import *
import bbq.analyse as analyse
import bbq.selector.strategy as strategy
import bbq.trade as trader
from hisql import *


def default(log_level='debug'):
    _, conf_dict = init_def_config()
    conf_dict['log']['level'] = log_level
    setup_log(conf_dict, 'bbq.log')
    fund_db = setup_db(conf_dict, FundDB)
    stock_db = setup_db(conf_dict, StockDB)
    mysql_db = hisql()
    mysql_db.connect(conf_dict['mysql']['uri'])
    return fund_db, stock_db, mysql_db


__all__ = ['__version__', '__author__', 'default',
           # hisql
           'hisql', 'One', 'Many', 'Affected', 'Scalar', 'Insert', 'Raw', 'DataFrame',
           # log.py
           'setup_logger', 'get_logger',
           # common.py
           'singleton', 'load_strategy', 'run_until_complete', 'setup_log', 'setup_db', 'load_cmd_yml',
           # bbq.config
           'init_def_config', 'init_config',
           # bbq.data
           'FundDB', 'StockDB',
           # bbq.fetch
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
           'is_trade_date',
           ]

globals().update({name: getattr(analyse, name) for name in analyse.__all__})
__all__.extend(analyse.__all__)

globals().update({name: getattr(strategy, name) for name in strategy.__all__})
__all__.extend(strategy.__all__)

globals().update({name: getattr(trader, name) for name in trader.__all__})
__all__.extend(trader.__all__)

