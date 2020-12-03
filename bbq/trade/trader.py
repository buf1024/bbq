import bbq.log as log
import click
import uuid
import asyncio
from bbq.common import setup_log, setup_db, run_until_complete, load_cmd_js
from bbq.data.mongodb import MongoDB
from bbq.data.stockdb import StockDB
from bbq.data.funddb import FundDB
from bbq.trade.tradedb import TradeDB
from bbq.config import init_config
from multiprocessing import Process
import multiprocessing as mp
from typing import Dict, Optional
import signal
from bbq.trade.account import Account
from bbq.trade.broker import init_broker, get_broker
from bbq.trade.risk import init_risk, get_risk
from bbq.trade.strategy import init_strategy, get_strategy
from datetime import datetime
from bbq.trade.strategy_info import StrategyInfo
import os
import sys


class Trader:
    def __init__(self, db_trade: TradeDB, db_data: MongoDB, config: Dict):
        self.log = log.get_logger(self.__class__.__name__)
        self.loop = asyncio.get_event_loop()
        self.db_trade = db_trade
        self.db_data = db_data
        self.config = config

        self.account = None

        self.queue = dict(
            quot_subs=asyncio.Queue(),
            quot_disp=asyncio.Queue(),
            signal=asyncio.Queue()
        )

        self.task_queue = asyncio.Queue()

        self.running = False

    async def start(self):
        await self.init_strategy()

        is_init = await self.init_account()
        if not is_init:
            self.log.error('初始化异常')
            return None

        self.running = True

        await self.task_queue.put('quot_subs_task')
        self.loop.create_task(self.quot_subs_task())

        await self.task_queue.put('quot_disp_task')
        self.loop.create_task(self.quot_disp_task())

        await self.task_queue.put('signal_task')
        self.loop.create_task(self.signal_task())

        await self.task_queue.put('risk_task')
        self.loop.create_task(self.risk_task())

        await self.task_queue.join()

    def stop(self):
        self.running = False

    def signal_handler(self, signum, frame):
        print('catch signal: {}, stop trade...'.format(signum))
        self.stop()

    async def create_new_account(self) -> Optional[Account]:
        typ = self.config['trade']['type']
        account = Account(typ=typ, account_id=str(uuid.uuid4()).replace('-', ''),
                          db_data=self.db_data, db_trade=self.db_trade)

        trade_dict = self.config['trade']
        account.kind = trade_dict['kind']

        account.cash_init = trade_dict['kind']
        account.cash_available = account.cash_init

        account.cost = 0
        account.profit = 0
        account.profit_rate = 0

        account.broker_fee = trade_dict['broker-fee'] if 'broker-fee' in trade_dict else 0.00025
        account.transfer_fee = trade_dict['transfer-fee'] if 'transfer-fee' in trade_dict else 0.00002
        account.tax_fee = trade_dict['tax_fee'] if 'tax_fee' in trade_dict else 0.001

        account.start_time = datetime.now()
        account.end_time = None

        strategy_id = trade_dict['strategy']['id'] if trade_dict['strategy'] is not None else None
        strategy_opt = trade_dict['strategy']['option'] if trade_dict['strategy'] is not None else None
        broker_id = trade_dict['broker']['id'] if trade_dict['broker'] is not None else None
        broker_opt = trade_dict['broker']['option'] if trade_dict['broker'] is not None else None
        risk_id = trade_dict['risk']['id'] if trade_dict['risk'] is not None else None
        risk_opt = trade_dict['risk']['option'] if trade_dict['risk'] is not None else None

        if broker_id is None:
            if typ in ['backtest', 'simulate']:
                broker_id = 'builtin:BrokerSimulate'
                broker_opt = {}
        if broker_id is None:
            self.log.error('broker_id not specific')
            return None

        cls = get_broker(broker_id)
        if cls is None:
            self.log.error('broker_id={} not data found'.format(broker_id))
            return None

        broker = cls(broker_id=broker_id, account=account)
        is_init = await broker.init(opt=broker_opt)
        if not is_init:
            self.log.error('init broker failed')
            return None
        account.broker = broker

        if risk_id is None:
            self.log.error('risk_id not specific')
            return None

        cls = get_risk(risk_id)
        if cls is None:
            self.log.error('risk_id={} not data found'.format(risk_id))
            return None
        risk = cls(risk_id=risk_id, account=account)
        is_init = await risk.init(opt=risk_opt)
        if not is_init:
            self.log.error('init risk failed')
            return None
        account.risk = risk

        if strategy_id is None:
            self.log.error('strategy_id not specific')
            return None

        cls = get_strategy(strategy_id)
        if cls is None:
            self.log.error('strategy_id={} not data found'.format(strategy_id))
            return None
        strategy = cls(strategy_id=strategy_id, account=account)
        is_init = await strategy.init(opt=strategy_opt)
        if not is_init:
            self.log.error('init strategy failed')
            return None
        account.strategy = strategy

        strategy_info = StrategyInfo(account=account)
        strategy_info.strategy_id = strategy_id
        strategy_info.strategy_opt = strategy_opt
        strategy_info.broker_id = broker_id
        strategy_info.broker_opt = broker_opt
        strategy_info.risk_id = risk_id
        strategy_info.risk_opt = risk_opt

        await account.sync_to_db()
        await strategy_info.sync_to_db()

        return account

    async def init_account(self):
        trade_dict = self.config['trade']
        acct_id, kind, typ = trade_dict['account-id'], trade_dict['kind'], trade_dict['type']
        if acct_id is not None and len(acct_id) > 0:
            # 直接load数据库
            accounts = await self.db_trade.load_account(filter=dict(status=0, kind=kind, type=typ, account_id=acct_id))
            if len(accounts) == 0:
                self.log.info('数据中没有已运行的real/simulate数据, account_id={}'.format(acct_id))
                return

            self.account = Account(account_id=acct_id, typ=typ, db_data=self.db_data, db_trade=self.db_trade)
            if not await self.account.sync_from_db():
                self.log.info('从数据库中初始或account失败, account_id={}'.format(acct_id))
                self.account = None
                return False
            return True

        strategy_js, risk_js, broker_js = trade_dict['strategy'], trade_dict['risk'], trade_dict['broker']
        if typ == 'real' or typ == 'simulate':
            if strategy_js is None and risk_js is None and broker_js is None:
                # fork 多个进程数据库存在的
                accounts = await self.db_trade.load_account(filter=dict(status=0, kind=kind, type=typ))
                if len(accounts) == 0:
                    self.log.info('数据中没有已运行的real/simulate数据')
                    return False
                path_dict = self.config['strategy']
                for account in accounts:
                    self.log.info('开始fork程序运行account_id={}'.format(account['account_id']))
                    p = Process(target=entry,
                                kwargs=dict(conf=None,
                                            log_path=self.config['log']['path'],
                                            log_level=self.config['log']['level'],
                                            uri=self.config['mongo']['uri'],
                                            pool=self.config['mongo']['pool'],
                                            strategy_path=self.config['strategy']['trade'],
                                            broker_path=self.config['strategy']['broker'],
                                            risk_path=self.config['strategy']['risk'],
                                            init_cash=0, transfer_fee=0, tax_fee=0, broker_fee=0,
                                            account_id=account['account_id'], trade_kind=kind,
                                            trade_type=typ,
                                            strategy=None, risk=None, broker=None),
                                daemon=False)
                    p.start()
                    self.log.info('process pid={}, alive={}'.format(p.pid, p.is_alive()))
                self.log.info('main process exit')
                sys.exit(0)

        # 新生成trade / backtest
        self.account = await self.create_new_account()
        if self.account is None:
            self.log.info('创建account失败')
            return None

        return True

    async def init_strategy(self):
        init_risk(self.config['strategy']['risk'])
        init_broker(self.config['strategy']['broker'])
        init_strategy(self.config['strategy']['trade'])

    async def quot_subs_task(self):
        await self.task_queue.get()
        while self.running:
            self.log.info('quot_subs_task...')
            await asyncio.sleep(1)

        self.task_queue.task_done()

    async def quot_disp_task(self):
        await self.task_queue.get()
        while self.running:
            self.log.info('quot_disp_task...')
            await asyncio.sleep(1)

        self.task_queue.task_done()

    async def signal_task(self):
        await self.task_queue.get()
        while self.running:
            self.log.info('signal_task...')
            await asyncio.sleep(1)

        self.task_queue.task_done()

    async def risk_task(self):
        await self.task_queue.get()
        while self.running:
            self.log.info('risk_task...')
            await asyncio.sleep(1)

        self.task_queue.task_done()


@click.command()
@click.option('--conf', type=str, help='config file, default location: ~')
@click.option('--log-path', type=str, default=None, help='log path')
@click.option('--log-level', type=str, default='debug', help='log level')
@click.option('--uri', type=str, default='mongodb://localhost:27017/', help='mongodb connection uri')
@click.option('--pool', default=10, type=int, help='mongodb connection pool size')
@click.option('--strategy-path', type=str, help='strategy extra path')
@click.option('--risk-path', type=str, help='risk extra path')
@click.option('--broker-path', type=str, help='broker extra path')
@click.option('--init-cash', type=float, help='init cash')
@click.option('--transfer-fee', type=float, default=0.00002, help='transfer fee')
@click.option('--tax-fee', type=float, default=0.001, help='tax fee')
@click.option('--broker-fee', type=float, default=0.00025, help='broker fee')
@click.option('--account-id', type=str, help='trade account_id')
@click.option('--trade-kind', type=str, default='stock', help='trade catalogue: stock,fund, default stock')
@click.option('--trade-type', type=str, default='simulate', help='trade type: real,simulate,backtest')
@click.option('--strategy', type=str, help='running trade strategy, js or base64 encode js')
@click.option('--risk', type=str, help='running risk strategy, js or base64 encode js, use default if not provide')
@click.option('--broker', type=str, help='broker config, js or base64 encode js, should provide if trade-type is real')
def main(conf: str, log_path: str, log_level: str,
         uri: str, pool: int,
         strategy_path: str, risk_path: str, broker_path: str,
         init_cash: float, transfer_fee: float, tax_fee: float, broker_fee: float,
         account_id: str, trade_kind: str, trade_type: str,
         strategy: str, risk: str, broker: str):
    mp.set_start_method('spawn')
    entry(conf=conf, log_path=log_path, log_level=log_level,
          uri=uri, pool=pool,
          strategy_path=strategy_path, risk_path=risk_path, broker_path=broker_path,
          init_cash=init_cash, transfer_fee=transfer_fee, tax_fee=tax_fee, broker_fee=broker_fee,
          account_id=account_id, trade_kind=trade_kind, trade_type=trade_type,
          strategy=strategy, risk=risk, broker=broker)


def entry(**opts):
    conf, log_path, log_level = opts['conf'], opts['log_path'], opts['log_level']
    uri, pool = opts['uri'], opts['pool']
    strategy_path, risk_path, broker_path = opts['strategy_path'], opts['risk_path'], opts['broker_path']
    init_cash = opts['init_cash']
    transfer_fee, tax_fee, broker_fee = opts['transfer_fee'], opts['tax_fee'], opts['broker_fee']
    account_id, trade_kind, trade_type = opts['account_id'], opts['trade_kind'], opts['trade_type']
    strategy, risk, broker = opts['strategy'], opts['risk'], opts['broker']

    if log_path is None:
        home = os.path.expanduser('~')
        log_path = home + os.sep + os.sep.join(['.config', 'bbq', 'logs'])

    strategy_path = strategy_path.split(',') if strategy_path is not None else None
    risk_path = risk_path.split(',') if risk_path is not None else None
    broker_path = broker_path.split(',') if broker_path is not None else None

    strategy_js = load_cmd_js(strategy) if strategy is not None else None
    risk_js = load_cmd_js(risk) if risk is not None else None
    broker_js = load_cmd_js(broker) if broker is not None else None
    if (strategy is not None and strategy_js is None) or \
            (risk is not None and risk_js is None) or \
            (broker is not None and broker_js is None):
        print('strategy cmd argument error')
        return

    conf_cmd = dict(log=dict(path=log_path, level=log_level),
                    mongo=dict(uri=uri, pool=pool),
                    strategy=dict(broker=broker_path, trade=strategy_path, risk=risk_path),
                    trade={'account-id': account_id, 'kind': trade_kind, 'type': trade_type,
                           'init-cash': init_cash,
                           'transfer-fee': transfer_fee, 'tax-fee': tax_fee, 'broker-fee': broker_fee,
                           'strategy': strategy_js, 'risk': risk_js, 'broker': broker_js})
    conf_file = None
    if conf is not None:
        _, conf_file = init_config(conf)
        if conf_file is None:
            print('config file: {} not exists'.format(conf))
            return
    conf_dict = conf_cmd

    if conf_file is not None:
        conf_dict = conf_file

    setup_log(conf_dict, 'trade.log')
    db_data = setup_db(conf_dict, StockDB if trade_kind == 'stock' else FundDB)
    db_trade = setup_db(conf_dict, TradeDB) if trade_type != 'backtest' else None

    if db_data is None or db_trade is None:
        return

    trader = Trader(db_trade=db_trade, db_data=db_data, config=conf_dict)
    signal.signal(signal.SIGTERM, trader.signal_handler)
    signal.signal(signal.SIGINT, trader.signal_handler)
    run_until_complete(trader.start())


if __name__ == '__main__':
    main()
