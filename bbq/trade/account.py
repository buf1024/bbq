from bbq.data.mongodb import MongoDB
from bbq.trade.tradedb import TradeDB
from bbq.trade.base_obj import BaseObj
from bbq.trade.util_fac import get_broker
from bbq.trade.util_fac import get_risk
from bbq.trade.entrust import Entrust
from bbq.trade.util_fac import get_strategy
from typing import Dict, Optional, Tuple
from bbq.trade.position import Position
from bbq.trade.deal import Deal
from datetime import datetime
import copy
from bbq.trade import event


class Account(BaseObj):
    def __init__(self, account_id: str, typ: str, db_data: MongoDB, db_trade: TradeDB, trader):
        super().__init__(typ=typ, db_data=db_data, db_trade=db_trade, trader=trader)

        self.account_id = account_id

        self.status = 0
        self.kind = ''

        self.cash_init = 0.0
        self.cash_available = 0.0
        self.cash_frozen = 0.0
        self.total_net_value = 0.0

        self.total_hold_value = 0.0
        self.cost = 0
        self.profit = 0
        self.profit_rate = 0

        self.broker_fee = 0.00025
        self.transfer_fee = 0.00002
        self.tax_fee = 0.001

        self.start_time = None
        self.end_time = None

        self.position = {}
        self.entrust = {}

        # 成交历史 backtest
        self.deal = []
        self.signal = []

        self.broker = None
        self.strategy = None
        self.risk = None

    async def sync_acct_from_db(self) -> Optional[Dict]:
        data = await self.db_trade.load_account(filter={'account_id': self.account_id, 'status': 0, 'type': self.typ},
                                                limit=1)
        if len(data) == 0:
            self.log.error('account_id={} not data found'.format(self.account_id))
            return None

        account = data[0]
        self.account_id = account['account_id']
        self.kind = account['kind']
        self.cash_init = account['cash_init']
        self.cash_available = account['cash_available']
        self.cash_frozen = account['cash_frozen']
        self.total_hold_value = account['total_hold_value']
        self.total_net_value = account['total_net_value']
        self.cost = account['cost']
        self.broker_fee = account['broker_fee']
        self.transfer_fee = account['transfer_fee']
        self.tax_fee = account['tax_fee']
        self.start_time = account['start_time']
        self.end_time = account['end_time']

        return account

    async def sync_strategy_from_db(self) -> bool:
        data = await self.db_trade.load_strategy(filter={'account_id': self.account_id},
                                                 limit=1)
        if len(data) == 0:
            self.log.error('account_id={} not strategy data found'.format(self.account_id))
            return False

        strategy = data[0]

        strategy_id = strategy['strategy_id']
        broker_id = strategy['broker_id']
        risk_id = strategy['risk_id']

        cls = get_broker(broker_id)
        if cls is None:
            self.log.error('broker_id={} not data found'.format(broker_id))
            return False
        self.broker = cls(broker_id=broker_id, account=self)
        is_init = await self.broker.sync_from_db()
        if not is_init:
            self.log.error('init broker failed')
            return False

        cls = get_risk(risk_id)
        if cls is None:
            self.log.error('risk_id={} not data found'.format(risk_id))
            return False
        self.risk = cls(risk_id=risk_id, account=self)
        is_init = await self.risk.sync_from_db()
        if not is_init:
            self.log.error('init risk failed')
            return False

        cls = get_strategy(strategy_id)
        if cls is None:
            self.log.error('strategy_id={} not data found'.format(strategy_id))
            return False
        self.strategy = cls(strategy_id=strategy_id, account=self)
        is_init = await self.strategy.sync_from_db()
        if not is_init:
            self.log.error('init strategy failed')
            return False

        return True

    async def sync_entrust_from_db(self) -> bool:
        now = datetime.now()
        trade_date = datetime(year=now.year, month=now.month, day=now.day)
        entrusts = await self.db_trade.load_entrust(
            filter={'account_id': self.account_id,
                    '$or': [{'status': 'init'}, {'status': 'commit'}, {'status': 'part_deal'}],
                    'time': {'$gte': trade_date}})
        for entrust_dict in entrusts:
            entrust = Entrust(entrust_id=entrust_dict['entrust_id'], account=self)
            entrust.from_dict(entrust_dict)
            self.entrust[entrust.entrust_id] = entrust

        entrusts = await self.db_trade.load_entrust(
            filter={'account_id': self.account_id,
                    '$or': [{'status': 'init'}, {'status': 'commit'}, {'status': 'part_deal'}],
                    'time': {'$lt': trade_date}})
        for entrust_dict in entrusts:
            entrust = Entrust(entrust_id=entrust_dict['entrust_id'], account=self)
            entrust.from_dict(entrust_dict)
            entrust.status = 'cancel'
            await entrust.sync_to_db()

        return True

    async def sync_position_from_db(self) -> bool:
        positions = await self.db_trade.load_position(filter={'account_id': self.account_id})
        for position in positions:
            pos = Position(position_id=position['position_id'], account=self)
            pos.from_dict(position)
            self.position[pos.code] = pos

        return True

    async def sync_from_db(self) -> bool:
        account = await self.sync_acct_from_db()
        if account is None:
            return False

        if not await self.sync_strategy_from_db():
            return False

        if not await self.sync_position_from_db():
            return False

        if not await self.sync_entrust_from_db():
            return False

        return True

    @BaseObj.discard_saver
    async def sync_to_db(self) -> bool:
        data = {'account_id': self.account_id, 'status': self.status,
                'kind': self.kind, 'type': self.typ,
                'cash_init': self.cash_init, 'cash_available': self.cash_available,
                'total_net_value': self.total_net_value, 'total_hold_value': self.total_hold_value, 'cost': self.cost,
                'broker_fee': self.broker_fee, "transfer_fee": self.transfer_fee, "tax_fee": self.tax_fee,
                'profit': self.profit, 'profit_rate': self.profit_rate,
                'start_time': self.start_time, 'end_time': self.end_time, 'update_time': datetime.now()}
        await self.db_trade.save_account(data=data)
        return True

    async def on_quot(self, evt, payload):
        # evt_start(backtest)
        # evt_morning_start evt_quotation evt_morning_end
        # evt_noon_start evt_quotation evt_noon_end
        # evt_end(backtest)
        self.log.info('account on quot, event={}, payload={}'.format(evt, payload))
        if evt == event.evt_morning_start or evt == event.evt_noon_start:
            self.is_trading = True
        if evt == event.evt_morning_end or evt == event.evt_noon_end:
            self.is_trading = False

        if evt == event.evt_noon_end:
            for position in self.position.values():
                if position.volume != position.volume_available:
                    position.volume_frozen = 0
                    position.volume_available = position.volume
                    await position.sync_to_db()
            for entrust in self.entrust.values():
                if entrust.status == 'commit':
                    entrust.status = 'cancel'
                    await entrust.sync_to_db()
            self.cash_available += self.cash_frozen
            await self.sync_to_db()

            if not self.trader.is_backtest():
                self.entrust.clear()

            await self.trader.daily_report()

        if evt == event.evt_quotation:
            await self.update_account(payload=payload)

    def get_position_volume(self, code) -> Tuple[int, int]:
        if code not in self.position:
            return 0, 0

        position = self.position[code]
        return position.volume, position.volume_available

    def get_fee(self, typ, code, price, volume) -> float:
        total = price * volume
        broker_fee = total * self.broker_fee
        if broker_fee < 5:
            broker_fee = 5
        tax_fee = 0
        if typ == 'buy':
            if code.startswith('sh6'):
                tax_fee = total * self.transfer_fee
        elif typ == 'sell':
            if code.startswith('sz') or code.startswith('sh'):
                tax_fee = total * self.tax_fee
        return round(broker_fee + tax_fee, 2)

    def get_cost(self, typ, code, price, volume) -> float:
        fee = self.get_fee(typ=typ, code=code, price=price, volume=volume)
        return round(fee + price * volume, 2)

    async def update_account(self, payload):
        self.profit = 0
        self.cost = 0
        self.total_hold_value = 0
        for position in self.position.values():
            if payload is not None and position.code in payload:
                await position.on_quot(payload)
            self.profit = self.profit + position.profit
            self.total_hold_value = round(self.total_hold_value + (position.now_price * position.volume), 2)
            self.cost = round(self.cost + (position.price * position.volume + position.fee), 2)
        self.profit = round(self.profit, 2)

        if self.cost > 0:
            self.profit_rate = round(self.profit / self.cost * 100, 2)
        self.total_net_value = self.cash_available + self.cash_frozen + self.total_hold_value
        await self.sync_to_db()

    async def on_signal(self, evt, payload):
        """
        payload.source  # 信号源: risk, strategy, broker, manual
        payload.signal  # sell, buy, cancel
        :param evt:  evt_sig_buy, evt_sig_sell, evt_sig_cancel
        :param payload:  TradeSignal / entrust_id
        :return:
        """
        self.log.info('account on_signal: evt={}, signal={}'.format(evt, payload))
        if not self.is_trading:
            self.log.info('is not on trading, omit signal')
            return

        sig = payload
        await sig.sync_to_db()
        if self.trader.is_backtest():
            self.signal.append(sig)

        if evt == event.evt_sig_buy or evt == event.evt_sig_sell:
            cost = 0
            if evt == event.evt_sig_buy:
                cost = self.get_cost(typ='buy',
                                     code=sig.code, price=sig.price, volume=sig.volume)
                if cost > self.cash_available:
                    self.log.warn('not enough money to buy, cost={}, available={}'.format(cost, self.cash_available))
                    return
                if sig.signal != 'buy':
                    self.log.error('signal not right, evt={}, signal={}'.format(evt, sig.signal))
                    return
            if evt == event.evt_sig_sell:
                volume, volume_available = self.get_position_volume(sig.code)
                if volume_available < sig.volume:
                    self.log.warn('not volume to sell, entrust={}, available={}'.format(sig.volume, volume_available))
                    return

                if sig.signal != 'sell':
                    self.log.error('signal not right, evt={}, signal={}'.format(evt, sig.signal))
                    return

            entrust = Entrust(self.get_uuid(), self)
            entrust.name = sig.name
            entrust.code = sig.code
            entrust.time = sig.time
            entrust.broker_entrust_id = ''
            entrust.type = sig.signal
            entrust.status = 'init'
            entrust.price = sig.price
            entrust.volume = sig.volume
            entrust.volume_deal = 0
            entrust.volume_cancel = 0
            entrust.signal = sig

            self.entrust[entrust.entrust_id] = entrust
            await entrust.sync_to_db()

            evt_broker = None
            if evt == event.evt_sig_buy:
                evt_broker = event.evt_broker_buy
                self.cash_frozen += cost
                self.cash_available -= cost

                await self.sync_to_db()
            elif evt == event.evt_sig_sell:
                evt_broker = event.evt_broker_sell
                position = self.position[sig.code]
                position.volume_available -= sig.volume
                position.volume_frozen += sig.volume

                await position.sync_to_db()
            if evt_broker is not None:
                await self.emit('broker', evt_broker, entrust)

        if evt == event.evt_broker_cancel:
            entrust = self.entrust[sig]
            await self.emit('broker', event.evt_broker_cancel, entrust)

    @staticmethod
    def update_position(position):
        position.max_price = position.now_price if position.now_price > position.max_price else position.max_price
        position.min_price = position.now_price if position.now_price < position.min_price else position.min_price

        position.profit = round((position.now_price - position.price) * position.volume - position.fee, 2)
        if position.profit > position.max_profit:
            position.max_profit = position.profit
            position.max_profit_time = position.time
        if position.profit < position.min_profit:
            position.min_profit = position.profit
            position.min_profit_time = position.time

        position.profit_rate = round(position.profit / (position.volume * position.price + position.fee) * 100, 2)
        if position.profit_rate > position.max_profit_rate:
            position.max_profit_rate = position.profit_rate
        if position.profit_rate < position.min_profit_rate:
            position.min_profit_rate = position.profit_rate

    async def add_position(self, deal):
        position = None
        if deal.code not in self.position:
            position = Position(self.get_uuid(), self)
            position.name = deal.name
            position.code = deal.code
            position.time = deal.time

            position.volume = deal.volume
            position.volume_available = 0

            position.fee = deal.fee
            position.price = deal.price

            position.now_price = deal.price

            self.update_position(position)

            await position.sync_to_db()
            self.position[position.code] = position

        else:
            position = self.position[deal.code]
            position.time = deal.time
            position.fee += deal.fee
            position.price = round((position.price * position.volume + deal.price * deal.volume) / (
                    position.volume + deal.volume), 4)
            position.volume += deal.volume

            position.now_price = deal.price
            self.update_position(position)

            await position.sync_to_db()

        self.cash_frozen = round(self.cash_frozen - self.get_cost('buy', position.code, deal.price, deal.volume), 2)
        await self.update_account(None)

    async def deduct_position(self, deal):
        if deal.code not in self.position:
            self.log.error('deduct_position code not found: {}'.format(deal.code))
            return
        position = self.position[deal.code]
        position.time = deal.time
        position.fee = round(position.fee + deal.fee, 2)
        position.volume -= deal.volume
        if position.volume > 0:
            position.now_price = deal.price
            position.volume_frozen -= deal.volume
            self.update_position(position)
            await position.sync_to_db()

        if position.volume == 0:
            del self.position[position.code]
            await self.db_data.delete_position(position)

        self.cash_available = round(self.cash_available + deal.volume * deal.price - deal.fee, 2)
        await self.update_account(None)

    async def on_broker(self, evt, payload):
        """

        :param evt:
        :param payload: Entrust
        :return:
        """
        self.log.info('account on_broker: evt={}, signal={}'.format(evt, payload))
        if evt == event.evt_broker_deal:
            entrust = copy.copy(payload)
            self.entrust[entrust.entrust_id] = entrust
            await entrust.sync_to_db()

            deal = Deal(self.get_uuid(), entrust.entrust_id, account=self)

            deal.entrust_id = entrust.entrust_id

            deal.name = entrust.name
            deal.code = entrust.code
            deal.time = entrust.time
            deal.type = entrust.type

            deal.price = entrust.price
            deal.volume = entrust.volume_deal

            deal.fee = self.get_fee(typ=entrust.type, code=deal.code, price=deal.price, volume=deal.volume)
            if self.trader.is_backtest():
                self.deal.append(deal)

            await deal.sync_to_db()
            if entrust.type == 'buy':
                await self.add_position(deal)
            elif entrust.type == 'sell':
                await self.deduct_position(deal)

            if entrust.volume == entrust.volume_deal + entrust.volume_cancel:
                if not self.trader.is_backtest():
                    del self.entrust[entrust.entrust_id]

        if evt == event.evt_broker_cancel:
            entrust = self.entrust[payload.entrust_id]
            entrust.volume_cancel = payload.volume_cancel
            if entrust.volume_deal != 0:
                entrust.status = 'part_deal'
            else:
                entrust.status = 'cancel'
            await entrust.sync_to_db()
            if not self.trader.is_backtest():
                del self.entrust[entrust.entrust_id]

        if evt == event.evt_broker_committed:
            entrust = self.entrust[payload.entrust_id]
            entrust.status = payload.status
            entrust.broker_entrust_id = payload.broker_entrust_id
            await entrust.sync_to_db()

    @staticmethod
    def get_obj_list(lst):
        data = []
        for obj in lst:
            data.append(obj.to_dict())
        return data

    def to_dict(self):
        return {'account_id': self.account_id, 'status': self.status,
                'kind': self.kind, 'type': self.typ,
                'cash_init': self.cash_init, 'cash_available': self.cash_available,
                'total_net_value': self.total_net_value, 'total_hold_value': self.total_hold_value, 'cost': self.cost,
                'broker_fee': self.broker_fee, "transfer_fee": self.transfer_fee, "tax_fee": self.tax_fee,
                'profit': self.profit, 'profit_rate': self.profit_rate,
                'start_time': self.start_time.strftime('%Y-%m-%d %H:%M:%S') if self.start_time is not None else None,
                'end_time': self.start_time.strftime('%Y-%m-%d %H:%M:%S') if self.start_time is not None else None,
                'position': self.get_obj_list(self.position.values()),
                'entrust': self.get_obj_list(self.entrust.values()),
                'deal': self.get_obj_list(self.deal),
                'signal': self.get_obj_list(self.signal)
                }
