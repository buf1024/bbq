import barbar.log as log
import click
from barbar.config import conf_dict
import os
from barbar.common import run_until_complete
import barbar.fetch as fetch
from barbar.data.stockdb import StockDB
from barbar.stock_nats import StockNats
import asyncio

from typing import Dict, ClassVar

from barbar.trade.account import Account
from barbar.trade.strategy import strategies


# from selector.strategy import strategies as select_strategies
# from trader.broker import brokers
# from trader.strategy import strategies as trade_strategies
# from trader.robot import robots

def run(uri: str, pool: int, nats: str, debug: bool, cls: ClassVar):
    uri = conf_dict['mongo']['uri'] if uri is None else uri
    pool = conf_dict['mongo']['pool'] if pool <= 0 else pool
    nats = conf_dict['nats']['uri'] if nats is None else nats

    file = None
    level = "critical"
    if debug:
        file = conf_dict['log']['path'] + os.sep + 'stock_trade.log'
        level = conf_dict['log']['level']

    log.setup_logger(file=file, level=level)
    logger = log.get_logger()
    logger.debug('初始化数据库')
    db = StockDB(uri=uri, pool=pool)
    if not db.init():
        print('初始化数据库失败')
        return

    fetch.init()

    trader = cls(options={'servers': [nats]}, db=db)
    trader.start()


class TradeReal(StockNats):
    def __init__(self, options: Dict, db: StockDB):
        self.loop = asyncio.get_event_loop()
        super().__init__(loop=self.loop, options=options)

        self.db = db

        self.topic_cmd = 'topic:barbar.tradeaccount.command'


class TradeBacktest(TradeReal):
    def __init__(self, options: Dict, db: StockDB):
        super().__init__(options=options, db=db)


class Trader(StockNats):

    def __init__(self, options: Dict, db: StockDB):
        self.loop = asyncio.get_event_loop()
        super().__init__(loop=self.loop, options=options)

        self.db = db

        self.topic_cmd = 'topic:barbar.trader.command'

        self.cmd_handlers = {
            'backtest': self.on_backtest,
            'trade': self.on_trade,
            'cancel': self.on_cancel
        }
        self.add_handler(self.topic_cmd, self.cmd_handlers)

        self.running = False

        self.strategies = []

        # account = Account(db=self.db)
        #
        # risk = Risk(account=account)
        # broker = Broker(account=account)
        # strategy = Strategy(account=account)
        #
        # while True：
        #     check_risk
        #
        # # broker.run()
        #     while True:
        #         xx = check_event()
        #         xx->onEvent()
        #
        # while True:
        #     quot = xx;
        #     signl = risk->on_quot...
        #
        #     onSignalxx
        #
        #
        # account->on_quot

    async def on_backtest(self, data):
        pass

    async def on_trade(self, data):
        pass

    async def on_cancel(self, data):
        pass

    def start(self):
        try:
            if self.loop.run_until_complete(self.nats_task()):
                self.loop.create_task(self.subscribe(self.topic_cmd))
                self.loop.run_forever()
        except Exception as e:
            self.log.error('quotation start 异常, ex={}, callstack={}'.format(e, traceback.format_exc()))
        finally:
            self.loop.close()


@click.command()
@click.option('--uri', type=str, help='mongodb connection uri')
@click.option('--pool', default=0, type=int, help='mongodb connection pool size')
@click.option('--nats', type=str, help='mongodb uri')
@click.option('--debug/--no-debug', default=True, help='show debug log')
def main(uri: str, pool: int, nats: str, debug: bool):
    run(uri=uri, pool=pool, nats=nats, debug=debug, cls=Trader)


if __name__ == '__main__':
    main()

# @singleton
# class TraderRepository(BaseRepository):
#     def __init__(self, config_path):
#         super().__init__(config_path)
#
#
# if __name__ == '__main__':
#     def help(broker=None, strategy=None, robot=None):
#         if broker is not None:
#             print('broker:')
#             print('name: {}\ndesc: {}\n'.format(broker.name, broker.desc))
#         if strategy is not None:
#             print('strategy:')
#             print('name: {}\ndesc: {}\n'.format(strategy.name, strategy.desc))
#         if robot is not None:
#             print('robot:')
#             print('name: {}\ndesc: {}\n'.format(robot.name, robot.desc))
#
#         if broker is None and strategy is None and robot is None:
#             print('brokers:')
#             for cls in brokers.values():
#                 print('name: {}\ndesc: {}\n'.format(cls.name, cls.desc))
#             print('strategies:')
#             for cls in trade_strategies.values():
#                 print('name: {}\ndesc: {}\n'.format(cls.name, cls.desc))
#             print('robots:')
#             for cls in robots.values():
#                 print('name: {}\ndesc: {}\n'.format(cls.name, cls.desc))
#
#
#     config_path, opts = parse_arguments(opt_desc='?=broker|strategy|robot=name '
#                                                  'strategy=name;arg1=v1;arg2=v2 '
#                                                  'broker=name;arg1=v1;arg2=v2 '
#                                                  'robot=name;arg1=v1;arg2=v2 ')
#     if config_path is None or opts is None:
#         print('parse_arguments failed')
#         os._exit(-1)
#
#     if '?' in opts:
#         topic = opts['?']
#         if not isinstance(topic, dict):
#             help()
#             os._exit(-1)
#
#         for k, v in topic.items():
#             if k == 'broker':
#                 names = brokers.keys()
#                 if v not in names:
#                     print('invalid broker name: {}, available names: {}'.format(v, names))
#                 else:
#                     help(broker=brokers[v])
#
#             if k == 'strategy':
#                 names = trade_strategies.keys()
#                 if v not in names:
#                     print('invalid strategy name: {}, available names: {}'.format(v, names))
#                 else:
#                     help(strategy=trade_strategies[v])
#
#             if k == 'robot':
#                 names = robots.keys()
#                 if v not in names:
#                     print('invalid robot name: {}, available names: {}'.format(v, names))
#                 else:
#                     help(robot=robots[v])
#
#         os._exit(0)
#
#     strategy_name = opts['strategy']['strategy']
#     broker_name = opts['broker']['broker']
#     robot_name = opts['robot']['robot']
#
#     strategy_cls = trade_strategies[strategy_name]
#     broker_cls = brokers[broker_name]
#     robot_cls = robots[robot_name]
#
#     repo = TraderRepository(config_path)
#     if not repo.init('trade'):
#         print('req init failed')
#         os._exit(-1)
#
#     broker = broker_cls(repo, **opts['broker'])
#     strategy = strategy_cls(repo, broker, **opts['strategy'])
#
#     trader = Trader(repo, 'trade')
#     trader.set_robot(robot_cls(repo, **opts['robot']))
#     trader.add_strategy(strategy)
#
#     trader.start()
#     trader.join()
#     bus_stop()
