import asyncio
import traceback
from abc import ABC
from datetime import datetime, timedelta
from functools import partial

from bbq import log
from bbq.data.mongodb import MongoDB
from bbq.fetch import is_trade_date


class CommSync(ABC):
    def __init__(self, ctx):
        self.ctx = ctx
        self.log = log.get_logger(self.__class__.__name__)

    @staticmethod
    def is_synced(start, end, sync_start_time_func=None):
        start = datetime(year=start.year, month=start.month, day=start.day) if start is not None else None
        end = datetime(year=end.year, month=end.month, day=end.day) if end is not None else None

        now = datetime.now()
        now_tag = datetime(year=now.year, month=now.month, day=now.day, hour=15, minute=30) \
            if sync_start_time_func is None else sync_start_time_func(now)

        is_synced = False
        if end == start and now < now_tag:
            is_synced = True

        if not is_synced:
            if start is not None and end is not None:
                while start <= end:
                    if is_trade_date(start):
                        break
                    start = start + timedelta(days=1)
                else:
                    is_synced = True

        return is_synced

    async def incr_sync_on_trade_date(self, query_func, fetch_func, save_func, key='trade_date',
                                      filter_data_func=None, sync_start_time_func=None):
        trade_date = await query_func()
        start = None
        end = datetime.now()
        if trade_date is not None:
            start = trade_date[key].iloc[0] + timedelta(days=1)

        is_synced = self.is_synced(start, end, sync_start_time_func)

        if not is_synced:
            if sync_start_time_func is not None:
                now_tag = sync_start_time_func(end)
                if end < now_tag:
                    end = end + timedelta(days=-1)
            data = fetch_func(start=start, end=end)
            data = filter_data_func(data) if filter_data_func is not None else data
            if data is not None and not data.empty:
                save_func = partial(save_func, data=data)
                await self.ctx.submit_db(save_func)

    @staticmethod
    def gen_incr_data(key, data_db, data):
        data_new = data
        if data_db is not None:
            diff_set = set(data[key].values).difference(set(data_db[key].values))
            if len(diff_set) > 0:
                diff_values = '["{}"]'.format('","'.join(diff_set))
                data_new = data.query('{} in {}'.format(key, diff_values))
            else:
                data_new = None
        return data_new

    async def incr_sync_on_code(self, query_func, fetch_func, save_func, key='code'):
        data = fetch_func()
        if data is None:
            return False

        data_db = await query_func()
        if data_db is None or data_db.shape[0] != data.shape[0]:
            data_new = self.gen_incr_data(key, data_db, data)
            if data_new is not None:
                await save_func(data=data_new)
        return data


class Task(CommSync):
    def __init__(self, ctx, name):
        super().__init__(ctx)
        self.name = name
        self.db = ctx.db

    async def task(self):
        pass

    async def run(self):
        try:
            self.log.info('开始运行task: {}'.format(self.name))
            await self.task()
        except Exception as e:
            self.log.error('运行task异常: ex={} stack={}'.format(e, traceback.format_exc()))
        finally:
            await self.ctx.queue.get()
            self.ctx.queue.task_done()


class DataSync(CommSync):
    def __init__(self, db: MongoDB, concurrent_fetch_count: int = 50, concurrent_save_count: int = 100, loop=None):
        super().__init__(self)

        self.db = db

        self.tasks = []

        self.concurrent_fetch_count = concurrent_fetch_count
        self.concurrent_save_count = concurrent_save_count
        self.loop = asyncio.get_event_loop() if loop is None else loop
        self.queue = asyncio.Queue(self.concurrent_fetch_count)
        self.queue_db = asyncio.Queue(self.concurrent_save_count)

    def add_task(self, task):
        self.tasks.append(task)

    async def prepare_tasks(self) -> bool:
        return True

    async def db_task(self, save_func):
        try:
            await self.queue_db.get()
            await save_func()
        except Exception as e:
            self.log.error('同步数据库异常: ex={} stack={}'.format(e, traceback.format_exc()))
        finally:
            self.queue_db.task_done()

    async def submit_db(self, save_func):
        await self.queue_db.put(True)
        self.loop.create_task(self.db_task(save_func))

    async def sync(self):
        try:
            if not await self.prepare_tasks():
                self.log.error('准备task失败')
                return None

            for task in self.tasks:
                await self.queue.put(task.name)
                self.log.info('准备运行task: {}'.format(task.name))
                self.loop.create_task(task.run())

            await self.queue.join()
            await self.queue_db.join()

        except Exception as e:
            self.log.error('同步数据失败: ex={}, stack={}'.format(e, traceback.format_exc()))