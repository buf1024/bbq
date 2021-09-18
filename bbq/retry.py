import traceback
import time
import types
from functools import wraps, partial
import bbq.log as log


def retry(func=None, *, attempts=3, sleep=5, name=None, prefix=None):
    if func is None:
        return partial(retry, attempts=attempts, sleep=sleep, name=name, prefix=prefix)

    def retry_call(arg0, *args, **kwargs):
        logger = log.get_logger(name=name, prefix=prefix)
        for i in range(attempts):
            try:
                if arg0 is None:
                    return func(*args, **kwargs)
                return func(arg0, *args, **kwargs)
            except Exception as e:
                msg = traceback.format_exc()
                logger.error('请求 {}, args={}, kwargs={}, 异常: \n{}'.format(func.__name__, args, kwargs, msg))
                if i + 1 == attempts:
                    break
                backoff = sleep ** (i + 1)
                logger.debug('请求 {} {}s后重试.'.format(func.__name__, backoff))
                time.sleep(backoff)
        return None

    @wraps(func)
    def wrapper_func(*args, **kwargs):
        return retry_call(None, *args, **kwargs)

    @wraps(func)
    def wrapper_cls(self, *args, **kwargs):
        return retry_call(self, *args, **kwargs)

    if isinstance(func, types.FunctionType) and len(func.__qualname__.split('.')) > 1:
        return wrapper_cls
    return wrapper_func


if __name__ == '__main__':
    class TestMe:
        @retry
        def retry_ok(self, a):
            print('a')

        @retry
        def retry_ex(self, a):
            raise Exception('error')


    t = TestMe()
    t.retry_ok(1)

    # t.retry_ex(2)

    @retry(attempts=2, sleep=10)
    def retry_x():
        print('retry x')
        raise Exception('ex')


    retry_x()

    print('ok')
