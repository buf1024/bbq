import traceback
import bbq.log as log
from abc import ABC
from functools import wraps
import time

from bbq.fetch.my_trade_date import is_trade_date
from datetime import datetime, timedelta


class BaseFetch(ABC):
    def __init__(self):
        self.log = log.get_logger(self.__class__.__name__)

    @staticmethod
    def retry_client(func):
        @wraps(func)
        def wrapper(self, *args, **kwargs):
            for i in range(3):
                try:
                    res = func(self, *args, **kwargs)
                    return res
                except Exception as e:
                    msg = traceback.format_exc()
                    self.log.error('请求 %s 异常: \n%s', func.__name__, msg)
                    self.log.debug('请求 %s {}s后重试.'.format((i + 1) * 6), func.__name__)
                    time.sleep((i + 1)**2 * 6)
            return None

        return wrapper

    @staticmethod
    def sina2xueqiu(code: str):
        mk, symbol = code[:2], code[2:]
        symbol = symbol + '.' + 'SH' if mk == 'sh' else symbol + '.' + 'SZ'
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
        while tmp < end:
            if is_trade_date(tmp):
                return True
            tmp = tmp + timedelta(days=1)
        return False
