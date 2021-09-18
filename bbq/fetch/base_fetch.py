from abc import ABC
from datetime import datetime, timedelta

import bbq.log as log
from bbq.fetch.my_trade_date import is_trade_date


class BaseFetch(ABC):
    def __init__(self):
        self.log = log.get_logger(self.__class__.__name__)

    @staticmethod
    def sina2xueqiu(code: str):
        mk, symbol = code[:2], code[2:]
        symbol = symbol + '.' + 'SH' if mk == 'sh' else symbol + '.' + 'SZ'
        return symbol

    @staticmethod
    def fund2xueqiu(code: str):
        symbol = code + '.' + 'SH' if code.startswith('5') else code + '.' + 'SZ'
        return symbol

    @staticmethod
    def xueqiu2sina(code: str):
        mk, symbol = code[-2:], code[:-3]
        return 'sh' + symbol if mk == 'SH' else 'sz' + symbol

    @staticmethod
    def is_trade(start: datetime, end: datetime) -> bool:
        if start is None or end is None:
            return True
        if end < start:
            return False
        tmp = start
        while tmp <= end:
            if is_trade_date(tmp):
                return True
            tmp = tmp + timedelta(days=1)
        return False

    @staticmethod
    def df_size(df):
        if df is None:
            return 0
        return df.shape[0]
