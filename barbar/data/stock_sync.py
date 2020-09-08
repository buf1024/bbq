from barbar import log
import traceback
from datetime import datetime, timedelta
import asyncio
import click
from barbar.data.stockdb import StockDB
from barbar.config import conf_dict
import os
from barbar.common import run_until_complete
import barbar.fetch as fetch


class StockSync:
    concurrent_count = 50

    def __init__(self, db: StockDB):
        self.log = log.get_logger(self.__class__.__name__)
        self.db = db

    async def sync(self):
        try:
            self.log.debug('开始同步股票数据')
            now = datetime.now()
            now = datetime(year=now.year, month=now.month, day=now.day)
            now_tag = datetime(year=now.year, month=now.month, day=now.day, hour=15, minute=30)
            codes, indexes = await self._sync_basic()
            if codes is None or indexes is None:
                self.log.error('保存/加载基础数据失败')
                return

            queue = asyncio.Queue(self.concurrent_count)
            loop = asyncio.get_event_loop()

            for _, code in codes.iterrows():
                code = code['code']
                await queue.put(code)
                coro = self.sync_code(now=now, now_tag=now_tag, code=code, queue=queue)
                loop.create_task(coro)

            for _, index in indexes.iterrows():
                code = index['code']
                await queue.put(index)
                coro = self.sync_index(now=now, now_tag=now_tag, code=code, queue=queue)
                loop.create_task(coro)

            await queue.put(now)
            coro = self.sync_block(queue=queue)
            loop.create_task(coro)

            await queue.join()

            # await self.db.build_index()
            self.log.debug('股票信息同步完成')
        except Exception as e:
            self.log.error('同步股票失败: ex={}, stack={}'.format(e, traceback.format_exc()))

    def _incr_data(self, key, data_db, data):
        data_new = data
        if data_db is not None:
            diff_set = set(data[key].values).difference(set(data_db['code'].values))
            if len(diff_set) > 0:
                diff_values = '["{}"]'.format('","'.join(diff_set))
                data_new = data.query('{} in {}'.format(key, diff_values))
                self.log.debug('增量数据新增个数: {}'.format(data_new.shape[0]))
            else:
                data_new = None
        return data_new

    async def _sync_basic(self):
        self.log.debug('开始同步基础数据...')

        self.log.debug('获取股票列表...')
        codes = fetch.get_code_list()
        if codes is None or codes.empty:
            self.log.error('获取股票列表错误')
            return None, None
        codes_db = await self.db.load_code_list(projection=['code'])
        if codes_db is None or codes_db.shape[0] != codes.shape[0]:
            codes_new = self._incr_data('code', codes_db, codes)
            if codes_new is not None:
                self.log.debug('保存股票列表, count = {} ...'.format(codes_new.shape[0]))
                await self.db.save_code_list(codes_new)

        self.log.debug('获取股票指数列表...')
        indexes = fetch.get_index_list()
        if indexes is None:
            self.log.error('获取股票指数列表')
            return None, None
        indexes_db = await self.db.load_index_list(projection=['code'])
        if indexes_db is None or indexes_db.shape[0] != indexes.shape[0]:
            indexes_new = self._incr_data('code', indexes_db, indexes)
            if indexes_new is not None:
                self.log.debug('保存股票列表, count = {} ...'.format(indexes_new.shape[0]))
                await self.db.save_index_list(indexes_new)

        self.log.debug('获取股票交易日历...')
        trad_cals = fetch.get_trade_cal()
        if trad_cals is None:
            self.log.error('获取交易日历错误')
            return None, None
        trad_cals_db = await self.db.load_trade_cal(projection=['cal_date'])
        if trad_cals_db is None or trad_cals_db.shape[0] != trad_cals.shape[0]:
            trad_cals_new = self._incr_data('cal_date', trad_cals_db, trad_cals)
            if trad_cals_new is not None:
                self.log.debug('保存股票交易日历...')
                await self.db.save_trade_cal(trad_cals_new)

        return codes, indexes

    async def _is_sync(self, now_tag: datetime, start, end):
        is_sync = True
        if end == start and datetime.now() < now_tag:
            is_sync = False

        if is_sync:
            ft = None
            if start is not None and end is not None:
                ft = {'is_open': 1, 'cal_date': {'$gte': datetime.strptime(start, '%Y%m%d'),
                                                 '$lte': datetime.strptime(end, '%Y%m%d')}}

            if ft is not None:
                trade_cals = await self.db.load_trade_cal(filter=ft)
                if trade_cals is None:
                    is_sync = False

        return is_sync

    async def sync_code(self, now: datetime, now_tag: datetime, code: str, queue: asyncio.Queue):
        try:
            self.log.debug('开始同步股票日线数据, code={}'.format(code))
            trade_dates = await self.db.load_stock_bar(code=code,
                                                       projection=['trade_date'],
                                                       sort=[('trade_date', -1)], limit=1)
            start = None
            end = now.strftime('%Y%m%d')
            if trade_dates is not None:
                start = trade_dates['trade_date'].iloc[0] + timedelta(days=1)
                start = start.strftime('%Y%m%d')

            is_sync = await self._is_sync(now_tag, start, end)

            if is_sync:
                day_bar = fetch.get_bar(code=code, start=start, end=end)

                if day_bar is not None and not day_bar.empty:
                    await self.db.save_stock_bar(code=code, data=day_bar)

            self.log.debug('开始同步股票复权因子数据, code={}'.format(code))
            trade_dates = await self.db.load_adj_factor(code=code,
                                                        projection=['trade_date'],
                                                        sort=[('trade_date', -1)], limit=1)
            if trade_dates is not None:
                start = trade_dates['trade_date'].iloc[0] + timedelta(days=1)
                start = start.strftime('%Y%m%d')

            is_sync = await self._is_sync(now_tag, start, end)

            if is_sync:
                adj_factor = fetch.get_adj_factor(code=code, start=start, end=end)
                if adj_factor is not None and not adj_factor.empty:
                    await self.db.save_adj_factor(code=code, data=adj_factor)

            self.log.debug('开始同步除权除息数据, code={}'.format(code))

            xdxr = fetch.get_xdxr_list(code=code)
            if xdxr is not None:
                xdxr_db = await self.db.load_xdxr_list(filter={'code': code})
                if xdxr_db is None or xdxr.shape[0] != xdxr_db.shape[0]:
                    xdxr = self._incr_data('code', xdxr_db, xdxr)
                    if xdxr is not None:
                        self.log.debug('保存股票除权除息信息...')
                        await self.db.save_xdxr_list(xdxr)
        except Exception as e:
            self.log.error('同步股票失败: code={} ex={} stack={}'.format(code, e, traceback.format_exc()))
        finally:
            await queue.get()
            queue.task_done()

    async def sync_index(self, now: datetime, now_tag: datetime, code: str, queue: asyncio.Queue):
        try:
            self.log.debug('开始同步指数日线数据, code={}'.format(code))
            trade_dates = await self.db.load_index_bar(code=code,
                                                       projection=['trade_date'],
                                                       sort=[('trade_date', -1)], limit=1)
            start = None
            end = now.strftime('%Y%m%d')
            if trade_dates is not None:
                start = trade_dates['trade_date'].iloc[0] + timedelta(days=1)
                start = start.strftime('%Y%m%d')

            is_sync = True
            if end == start and datetime.now() < now_tag:
                is_sync = False

            if is_sync:
                day_bar = fetch.get_index_bar(code=code, start=start, end=end)
                if day_bar is not None and not day_bar.empty:
                    await self.db.save_index_bar(code=code, data=day_bar)

        except Exception as e:
            self.log.error('同步指数失败: code={} ex={} stack={}'.format(code, e, traceback.format_exc()))
        finally:
            await queue.get()
            queue.task_done()

    async def sync_block(self, queue):
        try:
            self.log.debug('开始同步板块数据')

            block = fetch.get_block_list()

            if block is not None and not block.empty:
                await self.db.do_delete(self.db.block_info, just_one=False)
                await self.db.save_block_list(data=block)

        except Exception as e:
            self.log.error('同步板块数据失败: ex={} stack={}'.format(e, traceback.format_exc()))
        finally:
            await queue.get()
            queue.task_done()


@click.command()
@click.option('--uri', type=str, help='mongodb connection uri')
@click.option('--pool', default=0, type=int, help='mongodb connection pool size')
@click.option('--debug/--no-debug', default=True, help='show debug log')
def main(uri: str, pool: int, debug: bool):
    uri = conf_dict['mongo']['uri'] if uri is None else uri
    pool = conf_dict['mongo']['pool'] if pool <= 0 else pool

    file = None
    level = "critical"
    if debug:
        file = conf_dict['log']['path'] + os.sep + 'stock_sync.log'
        level = conf_dict['log']['level']

    log.setup_logger(file=file, level=level)
    logger = log.get_logger()
    logger.debug('初始化数据库')
    db = StockDB(uri=uri, pool=pool)
    if not db.init():
        print('初始化数据库失败')
        return

    fetch.init()

    sync = StockSync(db=db)
    run_until_complete(sync.sync())


if __name__ == '__main__':
    main()
