"""
一个乱来的股票投机分析工具
"""

__version__ = "0.0.1"
__author__ = "450171094@qq.com"

from bbq.log import *
from bbq.common import *
from bbq.retry import *

from bbq.config import *
from bbq.data import *
from bbq.fetch import *
from bbq.analyse import *
from hisql import *


def default(log_level='debug'):
    _, conf_dict = init_def_config()
    conf_dict['log']['level'] = log_level
    setup_log(conf_dict, 'bbq.log', True)
    fund_db = setup_db(conf_dict, FundDB)
    stock_db = setup_db(conf_dict, StockDB)
    mysql_db = hisql()
    mysql_db.connect(conf_dict['mysql']['uri'])
    return fund_db, stock_db, mysql_db

