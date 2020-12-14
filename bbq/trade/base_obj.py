import bbq.log as log
from bbq.data.mongodb import MongoDB
from bbq.trade.tradedb import TradeDB
from abc import ABC
from functools import wraps
import uuid
from typing import Dict
import yaml


class BaseObj(ABC):
    def __init__(self, typ: str, db_data: MongoDB, db_trade: TradeDB, trader):
        self.log = log.get_logger(self.__class__.__name__)

        self.typ = typ

        self.db_data = db_data
        self.db_trade = db_trade

        self.trader = trader

    @staticmethod
    def discard_saver(func):
        @wraps(func)
        async def wrapper(self, *args, **kwargs):
            if self.typ == 'backtest':
                return True
            return await func(self, *args, **kwargs)

        return wrapper

    @staticmethod
    def get_uuid():
        return str(uuid.uuid4()).replace('-', '')

    def emit(self, queue: str, evt: str, payload: object):
        self.trader.queue[queue].put_nowait((evt, payload))

    @property
    def is_trading(self):
        return self.trader.is_trading

    @is_trading.setter
    def is_trading(self, value):
        self.trader.is_trading = value

    async def sync_from_db(self) -> bool:
        return True

    async def sync_to_db(self) -> bool:
        return True

    def to_dict(self) -> Dict:
        return {}

    def __str__(self):
        return yaml.dump(self.to_dict())
