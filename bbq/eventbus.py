"""
event & task bus 模块

主要提供两个装饰器 on 和 task,
on 用于注册事件，可选线程或同步执行方式，事件通过emit触发。
task 用于注册后台任务，线程执行模式。

使用例子:
@on(event='test')
class testclass:
    def __call__(self, *args, **kwargs):
        print('test __call__')

@on(event='abc')
def testfunc(paylaod):
    print('abc func:', paylaod)

@task(every='minute', at='30')
def _task1():
    print('every minute at 30:', time.time())

@task(every='month', at='25:08:32:00')
def _task2():
    print('month at 25:08:32:00:', time.time())

@task(every='minute')
def _task3():
    print('minute', time.time())

@task(every='1s')
def _task4():
    print('1s', time.time())
    emit(event="test")

@task(every='mon', at='10:58:00')
def _task5():
    print('mon', time.time())

"""

from threading import Thread, Lock
from queue import Queue, Empty
from collections import defaultdict, namedtuple
from functools import wraps, partial
from datetime import datetime, timedelta
import time
import types


class _eventbase(Thread):
    Event = namedtuple('Event',
                       ['call', 'thread', 'event', 'every', 'at', 'concurrent', 'fired', 'last', 't', 'target',
                        'need_target'])

    def __init__(self):
        super().__init__()

        self.running = True

        self.events = defaultdict(list)
        self.event_funcs = {}

        self.lock = Lock()

    def add(self, event):
        name = event.event
        evts = self.events[name]

        for e in evts:
            if e.call.__qualname__ == event.call.__qualname__:
                return False

        with self.lock:
            evts.append(event)
            self.event_funcs[event.call.__qualname__] = name

        return True

    def delete(self, func=None):
        if func is None:
            return False

        if func.__qualname__ not in self.event_funcs:
            return False

        event = self.event_funcs[func.__qualname__]
        evts = self.events[event]

        for i, v in enumerate(evts):
            if v.call.__qualname__ == func.__qualname__:
                with self.lock:
                    evts.pop(i)
                    del self.event_funcs[func.__qualname__]
                    if len(evts) == 0:
                        del self.events[event]
                return True
        return False

    def bind(self, func=None, target=None):
        if func is None or target is None:
            return False

        if func.__qualname__ not in self.event_funcs:
            return False

        event = self.event_funcs[func.__qualname__]
        evts = self.events[event]

        for i, v in enumerate(evts):
            if v.call.__qualname__ == func.__qualname__:
                with self.lock:
                    v = v._replace(target=target, need_target=True)
                    evts[i] = v
                return True

        return False

    def size(self):
        return len(self.events)

    def launch(self, event, payload, etype):
        func, thread, *_, fired, last, t, target, need_target = event
        try:
            fired = fired + 1
            last = time.time()

            if need_target and target is None:
                print('warning:{} func {} not bind target'.format(datetime.now(), func.__qualname__))
                event = event._replace(fired=fired, last=last)
                return event

            if types.FunctionType != type(func):
                func = func()

            args = () if target is None else (target,)  # task
            if etype != 'task':
                args = (payload,) if target is None else (target, payload)

            if thread:
                t = Thread(target=func, args=args)
                t.start()
            else:
                func(*args)

            event = event._replace(fired=fired, last=last, t=t)

        except Exception as e:
            pass

        return event

    def stop(self):
        self.running = False

    def stat(self):
        d = {}
        for k, evts in self.events.items():
            d[k] = []
            for e in evts:
                d[k].append({'call': e.call.__qualname__, 'fired': e.fired, 'last': e.last})
        return d


class _event(_eventbase):
    _event_quit = '___event_quit____'

    def __init__(self):
        super().__init__()

        self.queue = Queue()
        self.start()

    def run(self):
        while self.running:
            event, payload = self.queue.get()
            print('event fire:', event)
            if event == self._event_quit:
                self.running = False
            if event in self.events:
                evts = self.events[event]
                for i, e in enumerate(evts):
                    event = self.launch(e, payload, 'event')
                    with self.lock:
                        evts[i] = event

    def add(self, func=None, event=None, thread=False, need_target=False):
        if func is None or event is None:
            return False

        ret = True
        events = event.split(',')
        for event in events:
            evt = super().Event(event=event, every=None, at=None, call=func,
                                thread=thread, concurrent=True, fired=0, last=0, t=None,
                                target=None, need_target=need_target)
            if not super().add(evt):
                ret = False

        return ret

    def put(self, event, payload=None):
        self.queue.put((event, payload), block=False)

    def stop(self):
        self.put(self._event_quit)


class _task(_eventbase):
    def __init__(self):
        super().__init__()

        self.queue = Queue()
        self.start()

    def run(self):
        while self.running:
            try:
                self.queue.get(timeout=1)
            except Empty:
                for evts in self.events.values():
                    for i, evt in enumerate(evts):
                        res = self._task_run(evt)
                        if res is not None:
                            with self.lock:
                                evts[i] = res

    def add(self, func=None, every=None, at=None, thread=True, concurrent=True, delay=False, need_target=False):
        if func is None or every is None:
            return False
        if not self._condition_check(every, at):
            return False

        event = str(every) + '_' + str(at)
        last = 0 if delay else time.time()
        evt = super().Event(event=event, every=every, at=at, call=func,
                            thread=thread, concurrent=concurrent, fired=0, last=last, t=None,
                            target=None, need_target=need_target)
        return super().add(evt)

    def launch(self, event, payload, etype):
        if event.t is None or event.concurrent:
            return super().launch(event, payload, etype)
        if not event.t.is_alive():
            return super().launch(event, payload, etype)
        return None

    def _task_run(self, event):
        *_, every, at, concurrent, fired, last, t, target, need_target = event

        digit, unit = self._unpack_every(every)
        now = time.time()

        if digit is not None:
            w = {'s': 1, 'm': 60, 'h': 60 * 60, 'd': 60 * 60 * 24, 'w': 60 * 60 * 24 * 7}
            sec = digit * w[unit]
            diff = now - last
            if diff >= sec:
                return self.launch(event, None, 'task')
            return None

        now = datetime.fromtimestamp(now)
        last = datetime.fromtimestamp(last)

        def minute():
            nonlocal at
            at = at if at is not None else '1'
            return timedelta(minutes=1, seconds=int(at))

        def hour():
            nonlocal at
            at = at if at is not None else '00:00'
            tm = time.strptime(at, '%M:%S')
            return timedelta(hours=1, minutes=tm.tm_min, seconds=tm.tm_sec)

        def day():
            nonlocal at
            at = at if at is not None else '00:00:00'
            tm = time.strptime(at, '%H:%M:%S')
            return timedelta(days=1, hours=tm.tm_hour, minutes=tm.tm_min, seconds=tm.tm_sec)

        def week():
            nonlocal at
            at = at if at is not None else 'mon:00:00:00'
            weeks = ['monday', 'tuesday', 'wednesday', 'thursday', 'friday', 'saturday', 'sunday',
                     'mon', 'tue', 'wed', 'thu', 'fri', 'sat', 'sun']
            idx = at.find(':')
            idx = weeks.index(at[:idx])
            idx = idx if idx <= 6 else idx - 7
            if now.weekday() != idx:
                return None
            tm = time.strptime(at[idx + 1:], '%H:%M:%S')
            return timedelta(days=7, hours=tm.tm_hour, minutes=tm.tm_min, seconds=tm.tm_sec)

        def month():
            nonlocal at
            at = at if at is not None else '1:00:00:00'
            idx = at.find(':')
            mday = int(at[:idx])
            if now.day != mday:
                return None
            tm = time.strptime(at[idx + 1:], '%H:%M:%S')
            nmonth = now.month + 1 if now.month <= 12 else 1
            nyear = now.year if now.month <= 12 else now.year + 1
            nxt = datetime(year=nyear, month=nmonth, day=mday, hour=0, minute=0, second=0)
            return nxt + timedelta(hours=tm.tm_hour, minutes=tm.tm_min, seconds=tm.tm_sec) - now

        def weekday(wday):
            if now.weekday() != wday:
                return None
            nonlocal at
            at = at if at is not None else '00:00:00'
            tm = time.strptime(at, '%H:%M:%S')
            return timedelta(days=7, hours=tm.tm_hour, minutes=tm.tm_min, seconds=tm.tm_sec)

        tab = {'minute': minute,
               'hour': hour,
               'day': day,
               'week': week,
               'month': month,
               'monday': partial(weekday, 0),
               'tuesday': partial(weekday, 1),
               'wednesday': partial(weekday, 2),
               'thursday': partial(weekday, 3),
               'friday': partial(weekday, 4),
               'saturday': partial(weekday, 5),
               'sunday': partial(weekday, 6),
               'mon': partial(weekday, 0),
               'tue': partial(weekday, 1),
               'wed': partial(weekday, 2),
               'thu': partial(weekday, 3),
               'fri': partial(weekday, 4),
               'sat': partial(weekday, 5),
               'sun': partial(weekday, 6)
               }

        delta = tab[unit]()
        if delta is None:
            return None

        diff = now - last
        if diff >= delta:
            return self.launch(event, None, 'task')
        return None

    @staticmethod
    def _unpack_every(every):
        d_idx = -1
        digit = None
        unit = every
        for i, c in enumerate(every):
            if c.isdigit():
                d_idx = i
                continue
            break
        if d_idx >= 0:
            digit = int(every[:d_idx + 1])
            unit = every[d_idx + 1:]

        return digit, unit

    def _condition_check(self, every, at):
        weeks = ['monday', 'tuesday', 'wednesday', 'thursday', 'friday', 'saturday', 'sunday',
                 'mon', 'tue', 'wed', 'thu', 'fri', 'sat', 'sun']
        words = ['minute', 'hour', 'day', 'week', 'month',
                 'monday', 'tuesday', 'wednesday', 'thursday', 'friday', 'saturday', 'sunday',
                 'mon', 'tue', 'wed', 'thu', 'fri', 'sat', 'sun']
        suffix = ['s', 'm', 'h', 'd', 'w']

        digit, unit = self._unpack_every(every)

        if digit is not None:
            if unit not in suffix:
                return False
            return True

        if unit not in words:
            return False

        if at is None:
            return True

        if unit == 'minute':
            try:
                time.strptime(at, '%S')
            except ValueError:
                return False
            return True
        if unit == 'hour':
            try:
                time.strptime(at, '%M:%S')
            except ValueError:
                return False
            return True
        if unit == 'week':
            idx = at.find(':')
            if idx <= 0:
                return False
            if at[:idx] not in weeks:
                return False
            try:
                time.strptime(at[idx + 1:], '%H:%M:%S')
            except ValueError:
                return False
            return True
        if unit == 'month':
            idx = at.find(':')
            if idx <= 0:
                return False
            if not at[:idx].isdigit():
                return False
            day = int(at[:idx])

            try:
                now = datetime.now()
                datetime(year=now.year, month=now.month, day=day, hour=0, minute=0, second=0)
                time.strptime(at[idx + 1:], '%H:%M:%S')
            except ValueError:
                return False
            return True

        try:
            time.strptime(at, '%H:%M:%S')
        except ValueError:
            return False
        return True


class eventbus:
    _def_eventbus = '_def_eventbus'
    _def_payload = '_def_payload'

    def __init__(self):
        self.eventq = defaultdict(lambda: _event())
        self.taskq = defaultdict(lambda: _task())

    @staticmethod
    def _add(queue, cat, **kwargs):
        cat = cat if cat is not None else eventbus._def_eventbus
        bus = queue[cat]
        return bus.add(**kwargs)

    @staticmethod
    def _del(queue, cat, **kwargs):
        cat = cat if cat is not None else eventbus._def_eventbus
        bus = None if cat not in queue else queue[cat]
        if bus is None:
            return False

        if not bus.delete(**kwargs):
            return False

        if bus.size() <= 0:
            bus.stop()
            del bus[cat]

        return True

    @staticmethod
    def _bind(queue, cat, **kwargs):
        cat = cat if cat is not None else eventbus._def_eventbus
        bus = None if cat not in queue else queue[cat]
        if bus is None:
            return False

        return bus.bind(**kwargs)

    @staticmethod
    def _stop(queue, cat, **kwargs):
        cat = cat if cat is not None else eventbus._def_eventbus
        bus = None if cat not in queue else queue[cat]
        if bus is None:
            return False

        if bus.stop(**kwargs):
            del bus[cat]
            return True
        return False

    @staticmethod
    def _stat(queue, cat, **kwargs):
        cat = cat if cat is not None else eventbus._def_eventbus
        bus = None if cat not in queue else queue[cat]
        if bus is None:
            return None

        return bus.stat(**kwargs)

    def evt_add(self, cat=None, func=None, event=None, thread=False, need_target=False):
        return eventbus._add(self.eventq, cat, func=func, event=event, thread=thread, need_target=need_target)

    def evt_del(self, cat=None, func=None):
        return eventbus._del(self.eventq, cat, func=func)

    def evt_bind(self, cat=None, func=None, target=None):
        return eventbus._bind(self.eventq, cat, func=func, target=target)

    def evt_stop(self, cat=None):
        return eventbus._stop(self.eventq, cat)

    def evt_stat(self, cat=None):
        return eventbus._stat(self.eventq, cat)

    def task_add(self, cat=None, func=None, every=None, at=None, thread=True, concurrent=True, delay=False,
                 need_target=False):
        return eventbus._add(self.taskq, cat, func=func, every=every, at=at, thread=thread, concurrent=concurrent,
                             delay=delay, need_target=need_target)

    def task_del(self, cat=None, func=None):
        return eventbus._del(self.taskq, cat, func=func)

    def task_bind(self, cat=None, func=None, target=None):
        return eventbus._bind(self.taskq, cat, func=func, target=target)

    def task_stop(self, cat=None):
        return eventbus._stop(self.taskq, cat)

    def task_stat(self, cat=None):
        return eventbus._stat(self.taskq, cat)

    def stop(self):
        for cat in self.eventq.keys():
            self.evt_stop(cat)

        for cat in self.taskq.keys():
            self.task_stop(cat)

        return True

    def emit(self, cat=None, event=None, payload=None):
        cat = cat if cat is not None else eventbus._def_eventbus
        if cat in self.eventq:
            self.eventq[cat].put(event, payload)


# 默认的全局bus，也可导入event自己搞
_bus = eventbus()


def on(func=None, *, cat=None, event=None, thread=False, **_):
    """
    注册事件装饰器
    :param func: 装饰的函数或实现__call__的类或类中的成员函数(需绑定target)
    :param cat: eventbus的类别
    :param event: event的名称
    :param thread: event发生时是否起线程调用
    :return:
    """
    if func is None:
        return partial(on, cat=cat, event=event, thread=thread, need_target=False)

    need_target = False if type(func) != types.FunctionType else len(func.__qualname__.split('.')) > 1
    _bus.evt_add(cat=cat, func=func, event=event, thread=thread, need_target=need_target)

    @wraps(func)
    def wrapper(*args, **kwargs):
        return func(*args, **kwargs)

    return wrapper


def task(func=None, *, cat=None, every=None, at=None, thread=True, concurrent=True, delay=False, **_):
    """
    注册task装饰器
    :param thread:
    :param func: 装饰的函数或实现__call__的类
    :param cat: eventbus的类别
    :param every: task调用的时间, 如1s, 1m, 1h, 1d, 1w
        如every带数量，那么at无效
    :param at: 执行的时间点every=minute, hour, day, week, month, monday~sunday,有效，
        at为every次级单位,如
        every=minute at 10
        every=hour at 05:10
        every=day at 20:05:10
        every=week at monday:20:05:10
        every=month at 15:20:05:10
        every=monday at 20:05:10

    :param concurrent: 是否顺序执行
    :param delay: 是否启动延迟
    :return:
    """
    if func is None:
        return partial(task, cat=cat, every=every, at=at, thread=thread, concurrent=concurrent,
                       delay=delay, need_target=False)

    need_target = False if type(func) != types.FunctionType else len(func.__qualname__.split('.')) > 1
    _bus.task_add(cat=cat, func=func, every=every, at=at, thread=thread, concurrent=concurrent,
                  delay=delay, need_target=need_target)

    @wraps(func)
    def wrapper(*args, **kwargs):
        return func(*args, **kwargs)

    return wrapper


def emit(cat=None, event=None, payload=None):
    """
    发送事件
    :param cat: eventbus的名称
    :param event: event的名称
    :param payload: event的参数
    :return:
    """
    if event is None:
        return False
    return _bus.emit(cat, event, payload)


def evt_add(cat=None, func=None, event=None, thread=False):
    return _bus.evt_add(cat, func, event, thread)


def evt_del(cat=None, func=None):
    return _bus.evt_del(cat, func)


def evt_bind(cat=None, func=None, target=None):
    return _bus.evt_bind(cat, func, target)


def evt_stop(cat=None):
    return _bus.evt_stop(cat)


def evt_stat(cat=None):
    return _bus.evt_stat(cat)


def task_add(cat=None, func=None, every=None, at=None, concurrent=True, delay=False):
    return _bus.task_add(cat, func, every, at, concurrent, delay)


def task_del(cat=None, func=None):
    return _bus.task_del(cat, func)


def task_bind(cat=None, func=None, target=None):
    return _bus.task_bind(cat, func, target)


def task_stop(cat=None):
    return _bus.task_stop(cat)


def task_stat(cat=None):
    return _bus.task_stat(cat)


def bus_stop():
    _bus.stop()


if __name__ == '__main__':
    # class _task3:
    #     def __init__(self):
    #         task_bind(func=self.target, target=self)
    #         print('init')
    #
    #     @task(every='3s', concurrent=False)
    #     def target(self):
    #         print('self={}, minute={}'.format(self, time.time()))
    #         time.sleep(5)
    #         print('sleep done')
    #
    #
    # @task(every='2s')
    # def fuc():
    #     print('func2s')

    class testclass:
        def __init__(self):
            self.a = 100
            print('testclass')
            evt_bind(func=self.testfunc, target=self)

        @on(event='abc')
        def testfunc(self, payload):
            print('abc cls func:', payload, self.a)


    @on(event='abc')
    def testfunc(payload):
        print('abc func:', payload)


    # t = _task3()
    emit(event='abc')

    t2 = testclass()
    emit(event='abc')
    input('wait...')
    evt_stop()
    task_stop()
    print(task_stat())
    print(evt_stat())
