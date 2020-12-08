import bbq.log as log
from bbq.data.mongodb import MongoDB
from bbq.trade.tradedb import TradeDB
from abc import ABC
from functools import wraps


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

    def emit(self, sig, payload):
        self.trader.queue['signal'].put_nowait((sig, payload))

    async def sync_from_db(self) -> bool:
        return True

    async def sync_to_db(self) -> bool:
        return True

