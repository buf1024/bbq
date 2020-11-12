import traceback
import bbq.log as log
from abc import ABC
from functools import wraps
import time


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
