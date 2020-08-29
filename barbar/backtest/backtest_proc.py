from trader.broker.broker_backtest import Broker
from trader.risk import strategy as risk_strategy
from trader import strategy as trade_strategy
from trader.trader import Trader
from trader.account import Account
from common import *


@singleton
class BacktestRepository(BaseRepository):
    def __init__(self, config_path):
        super().__init__(config_path)


if __name__ == '__main__':
    config_path, opts = parse_arguments()
    if config_path is None or opts is None:
        print('parse_arguments ailed')
        os._exit(-1)

    repo = BacktestRepository(config_path)
    if not repo.init('backtest'):
        print('req init failed')
        os._exit(-1)

    trader = Trader(repo,
                    mod='backtest',
                    start_date=datetime.strptime('20191216', '%Y%m%d'),
                    end_date=datetime.strptime('20191216', '%Y%m%d'),
                    stop_quot_end=True)

    broker = Broker(repo)
    strategy = trade_strategy['Dummy'](repo)
    strategy.add_codes(['000001.SZ'])
    risk = risk_strategy['Dummy'](repo)

    trader.add_barbar_trader(strategy, broker, risk)

    trader.start()
    trader.join()
    bus_stop()
