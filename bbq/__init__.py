"""
一个乱来的股票投机分析工具
"""

__version__ = "0.0.1"
__author__ = "450171094@qq.com"

from bbq.cmd.fund_sync import main as fund_sync
from bbq.cmd.stock_sync import main as stock_sync

from bbq.data.stockdb import StockDB
from bbq.data.funddb import FundDB

from bbq.fetch import *

