from datetime import datetime, timedelta
from functools import partial
from typing import Dict

import click

import bbq.fetch as fetch
from bbq.common import run_until_complete, setup_db, setup_log
from bbq.config import init_def_config
from bbq.data.data_sync import DataSync
from bbq.data.data_sync import Task
from bbq.data.stockdb import StockDB
from bbq.data.sql.mongo2sql import Mongo2Sql


@click.command()
@click.option('--uri', type=str, default='mongodb://localhost:27017/', help='mongodb connection uri')
@click.option('--pool', default=10, type=int, help='mongodb connection pool size')
@click.option('--mysql-uri', type=str, default='mysql+pymysql://bbq:bbq@localhost/bbq',
              help='mysql connection string')
@click.option('--function', type=str,
              help='sync one, split by ",", available: fund_info,fund_net,fund_daily,'
                   'stock_info,stock_daily,stock_index,stock_fq_factor,stock_index_info,'
                   'stock_index_daily,stock_ns_flow,stock_his_divend,stock_sw_index_info')
@click.option('--debug/--no-debug', default=True, type=bool, help='show debug log')
def main(uri: str = 'mongodb://localhost:27017/', pool: int = 5,
          mysql_uri: str = 'mysql+pymysql://bbq:bbq@localhost/bbq',
          function: str = None, debug: bool = True):
    _, conf_dict = init_def_config()
    conf_dict['mongo'].update(dict(uri=uri, pool=pool))
    conf_dict['log'].update(dict(level="debug" if debug else "critical"))
    setup_log(conf_dict, 'mongo2sql_sync.log')

    sync = Mongo2Sql()
    if not sync.init(mongo_uri=uri, mysql_uri=mysql_uri):
        print('init sync failed.')
        return
    run_until_complete(sync.sync(function))


if __name__ == '__main__':
    main()
