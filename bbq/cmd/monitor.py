from bbq.common import setup_log, setup_db, run_until_complete
from bbq.data.stockdb import StockDB
from bbq.data.funddb import FundDB
from bbq.config import *
import signal
import click
from bbq.monitor.monitor import Monitor


@click.command()
@click.option('--conf', type=str, default='.config/bbq/config.yml', help='config file, default location: ~')
def main(conf: str):
    conf_file, conf_dict = init_config(conf)
    if conf_file is None or conf_dict is None:
        print('config file: {} not exists / load yaml config failed'.format(conf))
        return
    setup_log(conf_dict, 'monitor.log')
    db_stock = setup_db(conf_dict, StockDB)
    db_fund = setup_db(conf_dict, FundDB)
    trader = Monitor(db_stock=db_stock, db_fund=db_fund, config=conf_dict)
    signal.signal(signal.SIGTERM, trader.signal_handler)
    signal.signal(signal.SIGINT, trader.signal_handler)
    run_until_complete(trader.start())


if __name__ == '__main__':
    main()
