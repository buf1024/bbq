from async_nats import AsyncNats
from data.source_mongo import SourceMongo
from os.path import dirname
import json
import os
import uuid
import traceback
import sys
import asyncio
from datetime import datetime, timedelta
import proto

from quot import quotes
from selector.strategy import strategies as selector_strategies
from trader.broker import brokers as trade_brokers
from trader.strategy import strategies as trade_strategies
from trader.risk import risks


class Barbarian(AsyncNats):
    file_path = dirname(__file__)

    def __init__(self, db_opt):
        super().__init__()

        self.db_opt = db_opt
        self.db = None

        self.topic_cmd = 'topic:barbarian.command'
        self.topic_backtest = 'topic:barbarian.backtest.>'
        self.topic_trade = 'topic:barbarian.trade.>'

        self.cmd_handlers = {
            'data': {},
            'doctor': {},
            'trade': {
                'run': self.on_trade_run
            },
            'select': {},
            'backtest': {
                'run': self.on_trade_run
            }
        }
        self.backtest_handlers = {
            'data': {},
            'doctor': {},
            'trade': {
                'status_report': self.on_trade_status_report,
                'update_codes': self.on_trade_update_codes
            },
            'select': {},
            'backtest': {
                'status_report': self.on_trade_status_report,
                'update_codes': self.on_trade_update_codes
            }
        }
        self.progs = {
            'data': os.sep.join([self.file_path, 'data', 'data_proc.py']),
            'doctor': os.sep.join([self.file_path, 'doctor', 'doctor_proc.py']),
            'select': os.sep.join([self.file_path, 'selector', 'selector_proc.py']),
            'trader': os.sep.join([self.file_path, 'trader', 'trader_proc.py']),
            'backtest': os.sep.join([self.file_path, 'trader', 'trader_proc.py'])
        }

        self.trader_cache = {}

    def start(self):
        self.db = SourceMongo()
        if not self.db.init(self.db_opt):
            self.log.error('connect db error')
            return

        super().start()

    async def trade_task(self, subject):
        quot = self.trader_cache[subject]['quot']
        interval = self.trader_cache[subject]['quot']['interval']
        try:
            self.log.info('{} start trade'.format(subject))
            trade_date_dict = {}
            trade_status = 'close'
            while self.nc.is_connected:
                now = datetime.now()
                now_date = now.strftime('%Y%m%d')
                if now_date not in trade_date_dict:
                    open_trade_date = self.db.load_trade_cal(
                        filter={'cal_date': datetime.strptime(now_date, '%Y%m%d')},
                        projection=['cal_date', 'is_open'], sort=[('cal_date', -1)], limit=1)
                    if open_trade_date is None:
                        self.log.error('为交易日历数据, date={}'.format(now_date))
                        return

                    is_open = open_trade_date['is_open'].tolist()[0]
                    trade_date_dict[now_date] = is_open

                is_open = trade_date_dict[now_date]
                if is_open == 0:
                    self.log.error('date={} 非交易日'.format(now_date))
                    await asyncio.sleep(60)
                    continue

                morning_start = datetime(year=now.year, month=now.month, day=now.day,
                                         hour=9, minute=30, second=0)
                morning_end = datetime(year=now.year, month=now.month, day=now.day,
                                       hour=11, minute=30, second=0)

                noon_start = datetime(year=now.year, month=now.month, day=now.day,
                                      hour=13, minute=0, second=0)
                noon_end = datetime(year=now.year, month=now.month, day=now.day,
                                    hour=14, minute=55, second=0)

                if morning_start <= now <= morning_end:
                    if trade_status == 'close':
                        # open signal
                        trade_status = 'morning_open'

                if morning_end <= now <= noon_start:
                    if trade_status == 'morning_open':
                        # close signal
                        trade_status = 'morning_close'

                if noon_start <= now <= noon_end:
                    if trade_status == 'morning_close':
                        # open signal
                        trade_status = 'noon_open'

                if now > noon_end:
                    if trade_status == 'noon_open':
                        # open signal
                        trade_status = 'close'

                if trade_status == 'close' or trade_status == 'morning_close':
                    await asyncio.sleep(10)

                sleep_time = interval
                codes = self.trader_cache[subject]['codes']
                quotes = quot.get_rt_quot(codes)
                if quotes is None:
                    self.log.debug('rt_quot == None, break')
                    break
                try:
                    payload = proto.request('trade', 'quot', dict(quot=quotes))
                    n1 = datetime.now()
                    await self.nc.request(subject, json.dumps(payload).encode(), 30)
                    n2 = datetime.now()

                    diff = n2 - n1
                    if diff.seconds >= interval:
                        self.log.warn('trade on quot 耗时过长: {}s', diff.seconds)
                        sleep_time = 0
                except Exception as e:
                    self.log.error('trade trade quot exception: e={}'.format(e))

                await asyncio.sleep(sleep_time)

            self.log.info('done trade')
            payload = proto.request('trade', 'done', None)
            await self.nc.publish(subject, json.dumps(payload).encode())
            await self.nc.flush()
            del self.trader_cache[subject]

        except Exception as e:
            self.log.error('backtest failed: subject={}, e={} call_stack={}'.format(subject, e, traceback.format_exc()))
        finally:
            if subject in self.trader_cache:
                del self.trader_cache[subject]

    async def backtest_task(self, subject):
        quot = self.trader_cache[subject]['quot']
        try:
            trade_date = None
            self.log.info('{} start backtest'.format(subject))
            while self.nc.is_connected:
                codes = self.trader_cache[subject]['codes']
                quotes = quot.get_rt_quot(codes)
                if quotes is None:
                    break
                keys = list(quotes.keys())
                quot_trade_date = quotes[keys[0]]['date']
                if quot_trade_date != trade_date:
                    if trade_date is not None:
                        try:
                            payload = proto.request('backtest', 'close', dict(period='noon', trade_date=trade_date))
                            await self.nc.request(subject, json.dumps(payload).encode(), 30)
                        except Exception as e:
                            self.log.error('backtest trade close exception: e={}'.format(e))
                    try:
                        payload = proto.request('backtest', 'open', dict(period='morning', trade_date=quot_trade_date))
                        await self.nc.request(subject, json.dumps(payload).encode(), 30)
                    except Exception as e:
                        self.log.error('backtest trade open exception: e={}'.format(e))

                    trade_date = quot_trade_date

                try:
                    payload = proto.request('backtest', 'quot', dict(quot=quotes))
                    await self.nc.request(subject, json.dumps(payload).encode(), 30)
                except Exception as e:
                    self.log.error('backtest trade quot exception: e={}'.format(e))

            if trade_date is not None:
                try:
                    payload = proto.request('backtest', 'close', dict(period='noon', trade_date=trade_date))
                    await self.nc.request(subject, json.dumps(payload).encode(), 30)

                except Exception as e:
                    self.log.error('backtest trade close exception: e={}'.format(e))

            self.log.info('done backtest')
            payload = proto.request('backtest', 'done', None)
            await self.nc.publish(subject, json.dumps(payload).encode())
            await self.nc.flush()
            del self.trader_cache[subject]

        except Exception as e:
            self.log.error('backtest failed: subject={}, e={} call_stack={}'.format(subject, e, traceback.format_exc()))
        finally:
            if subject in self.trader_cache:
                del self.trader_cache[subject]

    async def on_nats_connected(self):
        self.log.info('nats connected, listen subject: {}'.format(self.topic_cmd))
        await self.subscribe(self.topic_cmd)
        await self.subscribe(self.topic_backtest)
        await self.subscribe(self.topic_trade)

    async def on_nats_message(self, msg):
        subject, reply, data = msg.subject, msg.reply, msg.data.decode()
        self.log.info("Received a message on '{subject}': {data}".format(
            subject=subject, reply=reply, data=data))

        data = json.loads(data)
        handlers = None
        if subject == self.topic_cmd:
            handlers = self.cmd_handlers
        if subject.startswith(self.topic_backtest[:-1]):
            handlers = self.backtest_handlers
        if handlers is not None:
            await self.on_command(handlers=handlers, subject=subject, reply=reply, data=data)

    async def on_command(self, handlers, subject, reply, data):
        resp = proto.response(sid='', cat='', cmd='', status='ERR', msg='')
        try:
            sid, cat, cmd, options = data['id'], data['cat'], data['cmd'], data[
                'options'] if 'options' in data else None
            status, msg, options = await handlers[cat][cmd](subject, options)
            resp = proto.response(sid, cat, cmd, status, msg, options)
        except Exception as e:
            self.log.error('execute command failed: subject={}, data={} call_stack={}'.format(subject, data,
                                                                                              traceback.format_exc()))
            resp['msg'] = 'exception: {}'.format(e)
        finally:
            if reply != '':
                js_resp = json.dumps(resp)
                self.log.info("Response a message: '{data}'".format(data=js_resp))
                await self.nc.publish(subject=reply, payload=js_resp.encode())
                await self.nc.flush()

    async def on_trade_status_report(self, subject, options):
        status = options['status']
        if status == 'died':
            if subject in self.trader_cache:
                del self.trader_cache[subject]

        return 'OK', 'SUCCESS', None

    async def on_trade_update_codes(self, subject, options):
        if subject not in self.trader_cache:
            quot = quotes[options['quot']['name']](self.db)
            if not quot.init(options['quot']):
                self.log.error('init quot error: quot={}'.format(options['quot']))
                return
            self.trader_cache[subject] = {
                'codes': options['codes'],
                'interval': options['quot']['interval'],
                'quot': quot
            }
            if subject.startswith(self.topic_backtest[:-1]):
                self.loop.create_task(self.backtest_task(subject))
            if subject.startswith(self.topic_trade[:-1]):
                self.loop.create_task(self.trade_task(subject))
            return 'OK', 'SUCCESS', None

        codes = self.trader_cache[subject]['codes']
        self.trader_cache[subject]['codes'] = list(set(codes).union(set(options['codes'])))

        return 'OK', 'SUCCESS', None

    async def on_trade_run(self, subject, options):
        broker_name = options['broker']['name']
        risk_name = options['broker']['risk']['name']
        strategy_name = options['broker']['strategy']['name']
        quot_name = options['quot']['name']
        if strategy_name not in trade_strategies:
            ex = "trade strategy '{}' not found".format(strategy_name)
            self.log.error(ex)
            raise Exception(ex)
        if broker_name not in trade_brokers:
            ex = "broker '{}' not found".format(broker_name)
            self.log.error(ex)
            raise Exception(ex)
        if quot_name not in quotes:
            ex = "quote '{}' not found".format(quot_name)
            self.log.error(ex)
            raise Exception(ex)
        if risk_name not in risks:
            ex = "risk '{}' not found".format(risk_name)
            self.log.error(ex)
            raise Exception(ex)
        subject = '{prefix}.{strategy}.{broker}.{uuid}'.format(prefix=self.topic_backtest[:-2],
                                                               strategy=strategy_name,
                                                               broker=broker_name, uuid=str(uuid.uuid4()))
        self.log.info(
            'launch backtest proc: subject={} program={}, options={}'.format(subject, self.progs['backtest'], options))
        await asyncio.create_subprocess_exec(sys.executable, self.progs['backtest'], subject, json.dumps(options))
        return 'OK', 'SUCCESS', {'subject': subject}


if __name__ == '__main__':
    # if len(sys.argv) != 3:
    #     print('usage: python barbarian.py db_uri pool_size')
    #     sys.exit(-1)

    # db_opt = {
    #     'uri': sys.argv[1],
    #     'pool': sys.argv[2]
    # }
    db_opt = {
        'uri': 'mongodb://localhost:37017/',
        'pool': 5
    }
    barbarian = Barbarian(db_opt)
    barbarian.start()
