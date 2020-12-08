from bbq.data.mongodb import MongoDB
from bbq.trade.tradedb import TradeDB
from bbq.trade.base_obj import BaseObj
from bbq.trade.broker import get_broker
from bbq.trade.risk import get_risk
from bbq.trade.strategy import get_strategy
from typing import Dict, Optional
from bbq.trade.position import Position
from datetime import datetime


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
        self.deal = {}
        self.signal = {}

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
            self.position[pos.position_id] = pos

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

    async def update_position(self, position, payload):
        await position.on_quot(payload)
        self.profit += position.profit
        self.total_value += (position.now_price * position.volume)
        self.cost += (position.price * position.volume + position.fee)
        if self.cost > 0:
            self.profit_rate = self.profit / self.cost

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
                    await self.update_position(position, payload[position.code])

    async def on_broker(self, broker, payload):
        pass

    async def on_risk(self, risk, payload):
        pass

    async def on_strategy(self, strategy, payload):
        pass
