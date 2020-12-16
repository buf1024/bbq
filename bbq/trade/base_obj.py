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
            if self.trader.is_backtest():
                return True
            return await func(self, *args, **kwargs)

        return wrapper

    @staticmethod
    def get_uuid():
        return str(uuid.uuid4()).replace('-', '')

    async def emit(self, queue: str, evt: str, payload: object):
        if not self.trader.is_running(queue):
            self.log.error(
                'trade is stop running, still emit signal, omit: queue={}, evt={}, payload={}'.format(queue, evt,
                                                                                                      payload))
            return
        await self.trader.queue[queue].put((evt, payload))

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

    def from_dict(self, data: Dict):
        pass

    def to_dict(self) -> Dict:
        return {}

    def __str__(self):
        return yaml.dump(self.to_dict(), allow_unicode=True, sort_keys=False)
