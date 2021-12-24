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


async def bbq_plot(db, code, limit, start=None, end=None):
    """
    @param db:
    @param code:
    @param limit:
    @param start:
    @param end:
    """
    from datetime import datetime
    if start is None:
        start = datetime(year=1990, month=1, day=1)
    if end is None:
        now = datetime.now()
        end = datetime(year=now.year, month=now.month, day=now.day)

    df = None
    if isinstance(db, StockDB):
        df = await db.load_stock_daily(filter={'code': code, 'trade_date': {'$gte': start, '$lte': end}},
                                       limit=limit,
                                       sort=[('trade_date', -1)])
    elif isinstance(db, FundDB):
        df = await db.load_fund_daily(filter={'code': code, 'trade_date': {'$gte': start, '$lte': end}},
                                      limit=limit,
                                      sort=[('trade_date', -1)])

    else:
        mk = code[:2]
        if mk == 'sz' or mk == 'sh':
            df = db.execute(
                "select a.name, b.code, b.trade_date, "
                "b.close, b.open, b.high, b.low, b.volume, b.turnover, b.hfq_factor "
                "from stock_info a left join stock_daily b on a.code = b.code "
                "where a.code = :code and b.trade_date >= :start and b.trade_date <= :end "
                "order by b.trade_date desc limit :limit",
                DataFrame(), code=code, start=start, end=end, limit=limit)
        else:
            df = db.execute(
                "select a.name, b.code, b.trade_date, "
                "b.close, b.open, b.high, b.low, b.volume, b.turnover, b.hfq_factor "
                "from fund_info a left join fund_daily b on a.code = b.code "
                "where a.code = :code and b.trade_date >= :start and b.trade_date <= :end "
                "order by b.trade_date dec limit :limit",
                DataFrame(), code=code, start=start, end=end, limit=limit)

    if df is None:
        return None

    df = df.sort_values(by='trade_date', ascending=True)
    return my_plot(df)
