from bbq.data.mongodb import MongoDB
from bbq.trade.tradedb import TradeDB
from bbq.trade.base_obj import BaseObj
from bbq.trade.broker import get_broker
from bbq.trade.risk import get_risk
from bbq.trade.entrust import Entrust
from bbq.trade.strategy import get_strategy
from typing import Dict, Optional
from bbq.trade.position import Position
from bbq.trade.deal import Deal
from datetime import datetime
import copy
import json


class Account(BaseObj):
    def __init__(self, account_id: str, typ: str, db_data: MongoDB, db_trade: TradeDB, trader):
        super().__init__(typ=typ, db_data=db_data, db_trade=db_trade, trader=trader)

        self.account_id = account_id

        self.status = 0
        self.kind = ''

        self.cash_init = 0
        self.cash_available = 0

        self.total_value = 0
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
        self.total_value = account['total_value']
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

    async def sync_position_from_db(self) -> bool:
        positions = await self.db_trade.load_position(filter={'account_id': self.account_id})
        for position in positions:
            pos = Position(position_id=position['position_id'], account=self)
            if not pos.sync_from_db():
                return False
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

        return True

    @BaseObj.discard_saver
    async def sync_to_db(self) -> bool:
        data = {'account_id': self.account_id, 'status': self.status,
                'kind': self.kind, 'type': self.typ,
                'cash_init': self.cash_init, 'cash_available': self.cash_available,
                'total_value': self.total_value, 'cost': self.cost,
                'broker_fee': self.broker_fee, "transfer_fee": self.transfer_fee, "tax_fee": self.tax_fee,
                'profit': self.profit, 'profit_rate': self.profit_rate,
                'start_time': self.start_time, 'end_time': self.end_time, 'update_time': datetime.now()}
        await self.db_trade.save_account(data=data)
        return True

    async def update_account(self, position, payload):
        if payload is not None:
            await position.on_quot(payload)
        self.profit += position.profit
        self.profit = round(self.profit, 2)
        self.total_value += (position.now_price * position.volume)
        self.cost += (position.price * position.volume + position.fee)
        if self.cost > 0:
            self.profit_rate = round(self.profit / self.cost * 100, 2)
        self.cash_available = self.cash_init - self.cost
        await self.sync_to_db()

    async def on_quot(self, evt, payload):
        # evt_start(backtest)
        # evt_morning_start evt_quotation evt_morning_end
        # evt_noon_start evt_quotation evt_noon_end
        # evt_end(backtest)
        self.log.info('account on quot, event={}, payload={}'.format(evt, payload))
        if evt == 'evt_noon_end':
            # 日终处理
            pass
        if evt == 'evt_quotation':
            for position in self.position.values():
                if position.code in payload:
                    await self.update_account(position, payload[position.code])

    async def on_signal(self, evt, sig):
        await sig.sync_to_db()
        if self.trader.is_backtest():
            self.signal.append(sig)

        if sig.signal == 'buy':
            entrust = Entrust(self.get_uuid(), self)
            entrust.name = sig.name
            entrust.code = sig.code
            entrust.time = sig.time
            entrust.broker_entrust_id = ''  # broker对应的委托id
            entrust.type = sig.signal
            entrust.status = 'commit'
            entrust.price = sig.price
            entrust.volume = sig.volume
            entrust.volume_deal = 0
            entrust.volume_cancel = 0

            self.entrust[entrust.entrust_id] = entrust

            await entrust.sync_to_db()
            self.emit('broker', 'evt_buy', entrust)

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
            position.max_price = deal.price
            position.min_price = deal.price

            position.profit_rate = round(-deal.fee / (deal.volume * deal.price + deal.fee) * 100, 2)
            position.max_profit_rate = position.profit_rate
            position.min_profit_rate = position.profit_rate

            position.profit = round(-deal.fee, 2)
            position.max_profit = position.profit
            position.min_profit = position.profit

            position.max_profit_time = deal.time
            position.min_profit_time = deal.time

            await position.sync_to_db()
            self.position[position.code] = position
        else:
            position = self.position[deal.code]
            position.time = deal.time
            position.fee += deal.fee
            position.price = round((position.price * position.volume + deal.price * deal.volume) / (
                    position.volume + deal.volume), 2)
            position.volume += deal.volume

            position.now_price = deal.price
            position.max_price = deal.price if deal.price > position.max_price else position.max_price
            position.min_price = deal.price if deal.price < position.min_price else position.min_price

            position.profit = round((position.now_price - position.price) * position.volume - position.fee, 2)
            position.max_profit = position.profit if position.profit > position.max_profit else position.max_profit
            position.min_profit = position.profit if position.profit < position.min_profit else position.min_profit

            position.profit_rate = round(position.profit / (deal.volume * deal.price + deal.fee) * 100, 2)
            position.max_profit_rate = position.profit_rate if position.profit_rate > position.max_profit_rate else position.max_profit_rate
            position.min_profit_rate = position.profit_rate if position.profit < position.min_profit_rate else position.min_profit_rate

            await position.sync_to_db()

        await self.update_account(position, None)

    async def on_broker(self, evt, payload):
        if evt == 'evt_buy':
            entrust = copy.copy(payload)
            self.entrust[entrust.entrust_id] = entrust
            await entrust.sync_to_db()

            deal = Deal(self.get_uuid(), entrust.entrust_id, account=self)

            deal.entrust_id = entrust.entrust_id

            deal.name = entrust.name
            deal.code = entrust.code
            deal.time = entrust.time

            deal.price = entrust.price
            deal.volume = entrust.volume_deal

            total = deal.price * deal.volume
            broker_fee = total * self.broker_fee
            if broker_fee < 5:
                broker_fee = 5
            tax_fee = 0
            if deal.code.startswith('sh6'):
                tax_fee = total * self.transfer_fee
            deal.fee = round(broker_fee + tax_fee, 2)
            if self.trader.is_backtest():
                self.deal.append(deal)

            await deal.sync_to_db()
            await self.add_position(deal)

    async def on_risk(self, evt, payload):
        pass

    async def on_strategy(self, evt, payload):
        pass

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
                'total_value': self.total_value, 'cost': self.cost,
                'broker_fee': self.broker_fee, "transfer_fee": self.transfer_fee, "tax_fee": self.tax_fee,
                'profit': self.profit, 'profit_rate': self.profit_rate,
                'start_time': self.start_time.strftime('%Y-%m-%d %H:%M:%S') if self.start_time is not None else None,
                'end_time': self.start_time.strftime('%Y-%m-%d %H:%M:%S') if self.start_time is not None else None,
                'position:': self.get_obj_list(self.position.values()),
                'entrust': self.get_obj_list(self.entrust.values()),
                'deal': self.get_obj_list(self.deal),
                'signal': self.get_obj_list(self.signal)
                }

    def __str__(self):
        return json.dumps(self.to_dict(), indent=2)

