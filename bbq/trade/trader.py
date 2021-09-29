import bbq.log as log
import asyncio
import os
import tempfile
import subprocess as sub
from bbq.data.mongodb import MongoDB
from bbq.trade.tradedb import TradeDB
from typing import Dict, Optional
from bbq.trade.account import Account
from datetime import datetime, date
from bbq.trade.strategy_info import StrategyInfo
from bbq.trade.quotation import BacktestQuotation, RealtimeQuotation
from bbq.trade.msg.msg_push import MsgPush
from collections import defaultdict
import bbq.trade.consts as consts
from bbq.trade.report import Report
import yaml
import traceback
from bbq.trade.risk import *
from bbq.trade.broker import *
from bbq.trade.strategy import *


class Trader:
    def __init__(self, db_trade: TradeDB, db_data: MongoDB, config: Dict):
        self.log = log.get_logger(self.__class__.__name__)
        self.loop = asyncio.get_event_loop()
        self.db_trade = db_trade
        self.db_data = db_data
        self.config = config

        self.account = None

        self.queue = dict(
            account=asyncio.Queue(),
            risk=asyncio.Queue(),
            signal=asyncio.Queue(),
            strategy=asyncio.Queue(),
            broker=asyncio.Queue(),
            broker_event=asyncio.Queue(),
            quotation=asyncio.Queue(),
            robot=asyncio.Queue(),
        )

        self.task_queue = asyncio.Queue()

        self.quot = None

        self.running = False

        self.is_trading = False

        self.robot = None

        self.msg_push = MsgPush(trader=self)

        self.depend_task = defaultdict(int)

    def is_running(self, queue):
        if not self.running:
            return self.depend_task[queue] > 0

        return self.running

    def incr_depend_task(self, queue):
        self.depend_task[queue] += 1

    def decr_depend_task(self, queue):
        self.depend_task[queue] -= 1

    async def start(self):
        self.init_facility()

        is_init = await self.init_account()
        if not is_init:
            self.log.error('初始化账户异常')
            return None
        self.log.info('account inited')

        is_init = await self.init_quotation(opt=self.config['trade']['quotation'])
        if not is_init:
            self.log.error('初始化行情异常')
            return None
        self.log.info('quotation inited')

        if 'message' in self.config:
            is_init = self.msg_push.init(**self.config['message'])
            if not is_init:
                self.log.error('初始化推送信息异常')
                return None
            self.log.info('push message inited')

        self.running = True

        await self.task_queue.put('quot_task')
        self.loop.create_task(self.quot_task())

        await self.task_queue.put('quot_sub_task')
        self.loop.create_task(self.general_async_task('quotation', func=self.on_quot_sub))

        await self.task_queue.put('account_task')
        self.loop.create_task(self.account_task())

        await self.task_queue.put('signal_task')
        self.loop.create_task(self.general_async_task('signal', func=self.account.on_signal))

        await self.task_queue.put('risk_task')
        self.loop.create_task(self.general_async_task('risk',
                                                      func=self.account.risk.on_quot,
                                                      open_func=self.account.risk.on_open,
                                                      close_func=self.account.risk.on_close))

        await self.task_queue.put('strategy_task')
        self.loop.create_task(self.general_async_task('strategy',
                                                      func=self.account.strategy.on_quot,
                                                      open_func=self.account.strategy.on_open,
                                                      close_func=self.account.strategy.on_close))

        await self.task_queue.put('broker_task')
        self.loop.create_task(self.general_async_task('broker',
                                                      func=self.account.broker.on_entrust,
                                                      open_func=self.account.broker.on_open,
                                                      close_func=self.account.broker.on_close))

        await self.task_queue.put('broker_event_task')
        self.loop.create_task(self.general_async_task('broker_event',
                                                      func=self.account.on_broker))

        if self.robot is not None:
            await self.task_queue.put('robot_task')
            self.loop.create_task(self.general_async_task('robot',
                                                          func=self.robot.on_robot,
                                                          open_func=self.robot.on_open,
                                                          close_func=self.robot.on_close))

        await self.task_queue.join()

        if self.is_backtest():
            await self.backtest_report()

        self.log.info('trader done, exit!')

    async def destroy(self):
        await self.account.broker.destroy()
        await self.account.risk.destroy()
        await self.account.strategy.destroy()

    def stop(self):
        self.running = False
        for queue in self.queue.values():
            queue.put_nowait((consts.evt_term, None))

    def signal_handler(self, signum, frame):
        print('catch signal: {}, stop trade...'.format(signum))
        self.stop()

    def is_backtest(self) -> bool:
        typ = self.config['trade']['type']
        return typ == 'backtest'

    async def backtest_report(self):
        self.log.info('backtest report')
        print(self.account)

    async def daily_report(self):
        print('daily_report')

    async def trade_report(self):
        print('trade_report')
        report = Report(account=self.account)
        is_inited = await report.collect_data()
        if not is_inited:
            print('collect not data')
            return False

        plot = report.plot()
        plot.render('/Users/luoguochun/Downloads/render.html')

    async def on_quot_sub(self, evt, payload):
        if evt == 'evt_quot_codes' and self.running:
            codes = payload
            if len(codes) > 0:
                await self.quot.add_code(codes=codes)

    async def create_new_account(self) -> Optional[Account]:
        typ = self.config['trade']['type']
        account_id = self.config['trade']['account-id']
        if account_id is None:
            account_id = Account.get_uuid()
        if not self.is_backtest():
            data = await self.db_trade.load_account(filter={'account_id': account_id, 'status': 0, 'type': typ},
                                                    limit=1)
            if len(data) != 0:
                self.log.error(f'新建账户 {account_id} 已经存在')
                return None

        account = Account(typ=typ, account_id=account_id,
                          db_data=self.db_data, db_trade=self.db_trade, trader=self)

        trade_dict = self.config['trade']
        account.category = trade_dict['category']

        account.cash_init = trade_dict['init-cash']
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

        quot_opt = self.config['trade']['quotation'] if self.config['trade']['quotation'] is not None else None
        if quot_opt is not None:
            if 'start_date' in quot_opt and isinstance(quot_opt['start_date'], date):
                d = quot_opt['start_date']
                quot_opt['start_date'] = datetime(year=d.year, month=d.month, day=d.day)
            if 'end_date' in quot_opt and isinstance(quot_opt['end_date'], date):
                d = quot_opt['end_date']
                quot_opt['end_date'] = datetime(year=d.year, month=d.month, day=d.day)

            if self.is_backtest():
                account.start_time = quot_opt['start-date']

        if broker_id is None:
            if typ in ['backtest', 'simulate']:
                broker_id = 'builtin:BrokerSimulate'
                broker_opt = {}
        if broker_id is None:
            self.log.error('broker_id not specific')
            return None

        cls = get_broker(broker_id)
        if cls is None:
            self.log.error('broker_id={} not broker found'.format(broker_id))
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
        strategy_info.quot_opt = quot_opt

        await account.sync_to_db()
        await strategy_info.sync_to_db()

        return account

    async def init_account(self):
        trade_dict = self.config['trade']
        acct_id, cat, typ = trade_dict['account-id'], trade_dict['category'], trade_dict['type']
        if cat not in ['fund', 'stock'] or typ not in ['backtest', 'realtime', 'simulate']:
            self.log.error('category/type 不正确, category={}, type={}'.format(cat, typ))
            return False

        if not self.is_backtest() and (acct_id is not None and len(acct_id) > 0):
            # 直接load数据库
            accounts = await self.db_trade.load_account(
                filter=dict(status=0, category=cat, type=typ, account_id=acct_id))
            if len(accounts) > 0:
                self.account = Account(account_id=acct_id, typ=typ, db_data=self.db_data, db_trade=self.db_trade,
                                       trader=self)
                if not await self.account.sync_from_db():
                    self.log.info('从数据库中初始或account失败, account_id={}'.format(acct_id))
                    self.account = None
                    return False
                quot_opt = await self.db_trade.load_strategy(filter={'account_id': self.account.account_id},
                                                             projection=['quot_opt'], limit=1)
                quot_opt = None if len(quot_opt) == 0 else quot_opt[0]
                self.config['trade']['quotation'] = quot_opt['quot_opt']

                return True

        # strategy_js, risk_js, broker_js = trade_dict['strategy'], trade_dict['risk'], trade_dict['broker']
        if not self.is_backtest() and (acct_id is None or len(acct_id) == 0):
            # fork 多个进程数据库存在的, 多账号处理
            accounts = await self.db_trade.load_account(filter=dict(status=0, category=cat, type=typ))
            if len(accounts) == 0:
                self.log.info('数据中没有已运行的real/simulate数据')
                return False
            for account in accounts:
                self.log.info('开始fork程序运行account_id={}'.format(account['account_id']))
                _, path = tempfile.mkstemp()
                self.config['log']['file'] = 'trade-{}.log'.format(account['account_id'])
                self.config['trade']['account_id'] = account['account_id']
                with open(path, 'w') as f:
                    f.write(yaml.dump(self.config))
                trader = sub.Popen(['bbqtrader', '--conf', path])
                self.log.info('process pid={}'.format(trader.pid))
            self.log.info('main process exit')
            os._exit(0)

        # 新生成trade / backtest
        self.account = await self.create_new_account()
        if self.account is None:
            self.log.info('创建account失败')
            return None

        return True

    def init_facility(self):
        init_risk(self.config['trade']['strategy-path']['risk'])
        init_broker(self.config['trade']['strategy-path']['broker'])
        init_strategy(self.config['trade']['strategy-path']['trade'])

    async def init_quotation(self, opt) -> bool:
        if self.is_backtest():
            self.quot = BacktestQuotation(db=self.db_data)
        else:
            self.quot = RealtimeQuotation(db=self.db_data)

        return await self.quot.init(opt=opt)

    async def quot_task(self):
        await self.task_queue.get()
        is_backtest = self.is_backtest()
        while self.running:
            evt, payload = await self.quot.get_quot()
            if evt is not None:
                await self.queue['account'].put((evt, payload))

            sleep_sec = 1
            if is_backtest:
                if evt is None:
                    for key, queue in self.queue.items():
                        await queue.join()
                    self.stop()
                # 让出执行权
                sleep_sec = 0.01
            await asyncio.sleep(sleep_sec)
        self.task_queue.task_done()

    async def account_task(self):
        self.incr_depend_task('broker_event')
        self.incr_depend_task('broker')

        await self.task_queue.get()
        queue = self.queue['account']
        while self.is_running('account'):
            pre_evt, pre_payload = None, None
            if self.running:
                evt, payload = await queue.get()
            else:
                try:
                    evt, payload = queue.get_nowait()
                except Exception:
                    await asyncio.sleep(1)
                    continue

            if evt is not None and evt == consts.evt_term:
                queue.task_done()
                continue

            if not self.is_backtest():
                while not queue.empty():
                    self.log.warn('process is too slow...')
                    try:
                        if evt != consts.evt_quotation:
                            await self.account.on_quot(evt, payload)
                        else:
                            pre_evt, pre_payload = evt, payload
                    except Exception as e:
                        self.log.error('account task exception: {}, stack={}'.format(e, traceback.format_exc()))
                    finally:
                        evt, payload = queue.get()
                        queue.task_done()

            try:
                if pre_evt is not None:
                    if evt != consts.evt_quotation:
                        await self.account.on_quot(pre_evt, pre_payload)
                await self.account.on_quot(evt, payload)

                if evt != consts.evt_quotation:
                    await self.queue['broker'].put((evt, payload))
                    if self.robot is not None:
                        await self.queue['robot'].put((evt, payload))
                await self.queue['risk'].put((evt, payload))
                await self.queue['strategy'].put((evt, payload))
            except Exception as e:
                self.log.error('account task exception: {}, stack={}'.format(e, traceback.format_exc()))
            finally:
                queue.task_done()

        self.decr_depend_task('broker_event')
        self.decr_depend_task('broker')

        self.task_queue.task_done()

    async def general_async_task(self, queue, func, open_func=None, close_func=None):
        queue_name = queue
        if queue_name in ['signal', 'risk', 'strategy', 'robot']:
            self.incr_depend_task('account')

        await self.task_queue.get()
        queue = self.queue[queue]
        while self.is_running(queue_name):
            if self.running:
                evt, payload = await queue.get()
            else:
                try:
                    evt, payload = queue.get_nowait()
                except Exception:
                    await asyncio.sleep(1)
                    continue
            if evt is not None and evt == consts.evt_term:
                queue.task_done()
                continue

            try:
                evt_handled = False
                if open_func is not None:
                    if evt == consts.evt_morning_start or evt == consts.evt_noon_start or evt == consts.evt_start:
                        await open_func(evt, payload)
                        evt_handled = True
                if close_func is not None:
                    if evt == consts.evt_morning_end or evt == consts.evt_noon_end or evt == consts.evt_end:
                        await close_func(evt, payload)
                        evt_handled = True
                if not evt_handled and func is not None:
                    await func(evt, payload)
            except Exception as e:
                self.log.error('{} task exception: {}, stack={}'.format(queue_name, e, traceback.format_exc()))
            finally:
                queue.task_done()

        if queue_name in ['signal', 'risk', 'strategy', 'robot']:
            self.decr_depend_task('account')

        self.task_queue.task_done()
