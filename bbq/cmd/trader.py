from bbq.common import setup_log, setup_db, run_until_complete
from bbq.data.stockdb import StockDB
from bbq.data.funddb import FundDB
from bbq.config import *
import signal
import os
import click
from bbq.trade.trader import Trader
from bbq.trade.tradedb import TradeDB


@click.command()
@click.option('--conf', type=str, default='~/.config/bbq/config.yml', help='config file, default location: ~')
def main(conf: str):
    if conf is not None and '~' in conf:
        conf = os.path.expanduser(conf)
    conf_file, conf_dict = init_config(conf)
    if conf_file is None or conf_dict is None:
        print('config file: {} not exists / load yaml config failed'.format(conf))
        return
    cat, typ = conf_dict['trade']['category'], conf_dict['trade']['type']
    log_file = conf_dict['log']['file'] if 'file' in conf_dict['log'] else 'trade.log'
    setup_log(conf_dict, log_file)
    db_data = setup_db(conf_dict, StockDB if cat == 'stock' else FundDB)
    db_trade = setup_db(conf_dict, TradeDB) if typ != 'backtest' else None

    if db_data is None or (db_trade is None and typ != 'backtest'):
        print('data_db / trade_db init failed')
        return

    trader = Trader(db_trade=db_trade, db_data=db_data, config=conf_dict)
    signal.signal(signal.SIGTERM, trader.signal_handler)
    signal.signal(signal.SIGINT, trader.signal_handler)
    run_until_complete(trader.start())


if __name__ == '__main__':
    main()
