from bbq.data.funddb import FundDB
from bbq.data.stockdb import StockDB
import bbq.log as log
import pugsql
from functools import partial
from inspect import isgeneratorfunction
from os.path import dirname
import os
import asyncio
import traceback
from functools import wraps, partial


class Mongo2Sql:
    def __init__(self, loop=None):
        self.log = log.get_logger(self.__class__.__name__)
        self.fund_db = None
        self.stock_db = None
        self.sql_db = None

        self.tables = ['fund_info', 'fund_net', 'fund_daily',
                       'stock_info', 'stock_daily', 'stock_index', 'stock_margin',
                       'stock_concept',
                       'stock_fq_factor', 'stock_index_info', 'stock_index_daily',
                       'stock_ns_flow', 'stock_his_divend', 'stock_sw_index_info']
        self.queue = None
        self.loop = asyncio.get_event_loop() if loop is None else loop

    def init(self,
             mongo_uri='mongodb://localhost:27017/',
             mysql_uri='mysql+pymysql://bbq:bbq@localhost/bbq',
             concurrent_count=50):
        try:
            sql_dir = dirname(__file__) + os.sep + 'sync'
            self.sql_db = pugsql.module(sql_dir)
            self.sql_db.connect(mysql_uri)
            self.sql_db.test_connection()
            self.fund_db = FundDB(mongo_uri)
            self.stock_db = StockDB(mongo_uri)
            self.queue = asyncio.Queue(concurrent_count)
            if self.fund_db.init() and self.stock_db.init():
                return True
        except Exception as e:
            self.log.error('初始化失败: {}'.format(e))

        return False

    async def sync_one(self, sql_check_func, mongo_query_func, sql_save_func,
                       build_cond_func, build_none_cond_func=None, before_sql_save_func=None):
        sql_data = sql_check_func()
        cond = None
        if sql_data is None or (isgeneratorfunction(sql_data) and sql_data.next() is None):
            if build_none_cond_func is not None:
                cond = build_none_cond_func()
        else:
            cond = build_cond_func(sql_data)
        sync_data_fr = await mongo_query_func(filter=cond)
        if sync_data_fr is not None and not sync_data_fr.empty:
            sync_data_fr.fillna(value=0, inplace=True)
            sync_data = tuple(sync_data_fr.to_dict('records'))
            size = len(sync_data)
            self.log.info('正保存{}条记录到数据库'.format(size))
            start, end, step = 0, 0, 100
            # progress = 0.1
            # cal_proc = size > step
            if before_sql_save_func is not None:
                with self.sql_db.transaction():
                    before_sql_save_func()
            while end < size:
                end = end + step
                save_data = sync_data[start:] if end > size else sync_data[start:end]
                with self.sql_db.transaction():
                    sql_save_func(*save_data)
                start = end

                # if cal_proc:
                #     p = end / size
                #     if p > progress:
                #         self.log.info('已经保存{:.2f}%, 程序没挂，耐心等待'.format(round(p, 4) * 100))
                #         progress = p + 0.1
            self.log.info('已保存{}条记录到数据库'.format(size))

    def sync_wrap(func):
        @wraps(func)
        async def wrapper(self, *args, **kwargs):
            try:
                res = await func(self, *args, **kwargs)
                await self.queue.get()
                return res
            except Exception as e:
                self.log.error('运行task异常: ex={} stack={}'.format(e, traceback.format_exc()))
            finally:
                self.queue.task_done()

        return wrapper

    @sync_wrap
    async def sync_fund_info(self):
        self.log.info('开始同步基金代码')
        await self.sync_one(sql_check_func=self.sql_db.select_fund_codes,
                            mongo_query_func=self.fund_db.load_fund_info,
                            sql_save_func=self.sql_db.insert_fund_info,
                            build_cond_func=lambda data: {'code': {'$not': {'$in': [it['code'] for it in data]}}})
        self.log.info('同步基金代码完成')

    @sync_wrap
    async def sync_fund_net(self, code):
        self.log.info('开始同步基金{}净值'.format(code))
        await self.sync_one(sql_check_func=partial(self.sql_db.select_fund_net, code=code),
                            mongo_query_func=partial(self.fund_db.load_fund_net, sort=[('trade_date', 1)]),
                            sql_save_func=self.sql_db.insert_fund_net,
                            build_cond_func=lambda data: {'trade_date': {'$gt': data['trade_date']},
                                                          'code': code},
                            build_none_cond_func=lambda: {'code': code})
        self.log.info('同步基金{}净值完成'.format(code))

    @sync_wrap
    async def sync_fund_daily(self, code):
        self.log.info('开始同步基金{}日线'.format(code))
        await self.sync_one(sql_check_func=partial(self.sql_db.select_fund_daily, code=code),
                            mongo_query_func=partial(self.fund_db.load_fund_daily,
                                                     sort=[('trade_date', 1)]),
                            sql_save_func=self.sql_db.insert_fund_daily,
                            build_cond_func=lambda data: {'trade_date': {'$gt': data['trade_date']},
                                                          'code': code},
                            build_none_cond_func=lambda: {'code': code})
        self.log.info('同步基金{}日线完成'.format(code))

    @sync_wrap
    async def sync_stock_info(self):
        self.log.info('开始同步股票代码')
        await self.sync_one(sql_check_func=self.sql_db.select_stock_codes,
                            mongo_query_func=self.stock_db.load_stock_info,
                            sql_save_func=self.sql_db.insert_stock_info,
                            build_cond_func=lambda data: {'code': {'$not': {'$in': [it['code'] for it in data]}}})
        self.log.info('同步股票代码完成')

    @sync_wrap
    async def sync_stock_daily(self, code):
        self.log.info('开始同步股票{}日线'.format(code))
        await self.sync_one(sql_check_func=partial(self.sql_db.select_stock_daily, code=code),
                            mongo_query_func=partial(self.stock_db.load_stock_daily,
                                                     sort=[('trade_date', 1)]),
                            sql_save_func=self.sql_db.insert_stock_daily,
                            build_cond_func=lambda data: {'trade_date': {'$gt': data['trade_date']},
                                                          'code': code},
                            build_none_cond_func=lambda: {'code': code})
        self.log.info('同步股票{}日线完成'.format(code))

    @sync_wrap
    async def sync_stock_index(self, code):
        self.log.info('开始同步股票{}指标'.format(code))
        await self.sync_one(sql_check_func=partial(self.sql_db.select_stock_index, code=code),
                            mongo_query_func=partial(self.stock_db.load_stock_index,
                                                     sort=[('trade_date', 1)]),
                            sql_save_func=self.sql_db.insert_stock_index,
                            build_cond_func=lambda data: {'trade_date': {'$gt': data['trade_date']},
                                                          'code': code},
                            build_none_cond_func=lambda: {'code': code})
        self.log.info('同步股票{}指标完成'.format(code))

    @sync_wrap
    async def sync_stock_fq_factor(self, code):
        self.log.info('开始同步股票{}复权因子'.format(code))
        await self.sync_one(sql_check_func=partial(self.sql_db.select_stock_fq_factor, code=code),
                            mongo_query_func=partial(self.stock_db.load_stock_fq_factor,
                                                     sort=[('trade_date', 1)]),
                            before_sql_save_func=partial(self.sql_db.delete_stock_fq_factor, code=code),
                            sql_save_func=self.sql_db.insert_stock_fq_factor,
                            build_cond_func=lambda data: {'trade_date': {'$gt': data['trade_date']},
                                                          'code': code},
                            build_none_cond_func=lambda: {'code': code})
        self.log.info('同步股票{}复权因子完成'.format(code))

    @sync_wrap
    async def sync_stock_margin(self, code):
        self.log.info('开始同步股票{}融资融券数据'.format(code))
        await self.sync_one(sql_check_func=partial(self.sql_db.select_stock_margin, code=code),
                            mongo_query_func=partial(self.stock_db.load_stock_margin, sort=[('trade_date', 1)]),
                            sql_save_func=self.sql_db.insert_stock_margin,
                            build_cond_func=lambda data: {'trade_date': {'$gt': data['trade_date']},
                                                          'code': code},
                            build_none_cond_func=lambda: {'code': code})
        self.log.info('同步股票{}融资融券数据完成'.format(code))

    @sync_wrap
    async def sync_stock_index_info(self):
        self.log.info('开始同步股票指数代码')
        await self.sync_one(sql_check_func=self.sql_db.select_index_info_codes,
                            mongo_query_func=self.stock_db.load_index_info,
                            sql_save_func=self.sql_db.insert_stock_index_info,
                            build_cond_func=lambda data: {'code': {'$not': {'$in': [it['code'] for it in data]}}})
        self.log.info('同步股票指数代码完成')

    @sync_wrap
    async def sync_stock_index_daily(self, code):
        self.log.info('开始同步股票指数{}日线'.format(code))
        await self.sync_one(sql_check_func=partial(self.sql_db.select_stock_index_daily, code=code),
                            mongo_query_func=partial(self.stock_db.load_index_daily,
                                                     sort=[('trade_date', 1)]),
                            sql_save_func=self.sql_db.insert_stock_index_daily,
                            build_cond_func=lambda data: {'trade_date': {'$gt': data['trade_date']},
                                                          'code': code},
                            build_none_cond_func=lambda: {'code': code})
        self.log.info('同步股票指数{}日线完成'.format(code))

    @sync_wrap
    async def sync_stock_ns_flow(self):
        self.log.info('开始同步股票南北资金流')
        await self.sync_one(sql_check_func=self.sql_db.select_stock_ns_flow,
                            mongo_query_func=partial(self.stock_db.load_stock_north_south_flow,
                                                     sort=[('trade_date', 1)]),
                            sql_save_func=self.sql_db.insert_stock_ns_flow,
                            build_cond_func=lambda data: {'trade_date': {'$gt': data['trade_date']}})
        self.log.info('同步股票南北资金流完成')

    @sync_wrap
    async def sync_stock_his_divend(self):
        self.log.info('开始历史分红数据')
        await self.sync_one(sql_check_func=self.sql_db.select_stock_his_divend,
                            mongo_query_func=partial(self.stock_db.load_stock_his_divend, sort=[('trade_date', 1)]),
                            before_sql_save_func=self.sql_db.delete_stock_his_divend,
                            sql_save_func=self.sql_db.insert_stock_his_divend,
                            build_cond_func=lambda data: {'sync_date': {'$gt': data['sync_date']}})
        self.log.info('历史分红数据完成')

    @sync_wrap
    async def sync_sw_index_info(self):
        self.log.info('开始同步申万行业数据')
        await self.sync_one(sql_check_func=self.sql_db.select_stock_sw_index_info_codes,
                            mongo_query_func=self.stock_db.load_sw_index_info,
                            sql_save_func=self.sql_db.insert_stock_sw_index_info,
                            build_cond_func=lambda data: {
                                'index_code': {'$not': {'$in': [it['index_code'] for it in data]}}})
        self.log.info('同步申万行业数据完成')

    @sync_wrap
    async def sync_stock_concept(self):
        self.log.info('开始同步股票概念数据')
        await self.sync_one(sql_check_func=self.sql_db.select_stock_concept,
                            mongo_query_func=self.stock_db.load_stock_concept,
                            sql_save_func=self.sql_db.insert_stock_concept,
                            build_cond_func=lambda data: {
                                'concept_code': {'$not': {'$in': [it['concept_code'] for it in data]}}})
        self.log.info('同步股票概念数据完成')

    async def add_task(self, name, coro):
        await self.queue.put(name)
        self.loop.create_task(coro)

    async def sync(self, tables=None):
        sync_tables = self.tables
        if tables is not None:
            sync_tables = []
            tables = tables.split(',')
            for table in tables:
                if table not in self.tables:
                    self.log.error('同步表{}不存在'.format(table))
                    return None
                sync_tables.append(table)
        if 'fund_info' in sync_tables:
            await self.add_task('fund_info', self.sync_fund_info())

        if 'fund_daily' in sync_tables or 'fund_net' in sync_tables:
            codes = await self.fund_db.load_fund_info(projection=['code'])
            for code in codes.to_dict('records'):
                code = code['code']
                if 'fund_net' in sync_tables:
                    await self.add_task('fund_net_{}'.format(code), self.sync_fund_net(code=code))

                if 'fund_daily' in sync_tables:
                    await self.add_task('fund_daily_{}'.format(code), self.sync_fund_daily(code=code))

        if 'stock_info' in sync_tables:
            await self.add_task('stock_info', self.sync_stock_info())

        if 'stock_daily' in sync_tables or \
                'stock_index' in sync_tables or \
                'stock_fq_factor' in sync_tables:
            codes = await self.stock_db.load_stock_info(projection=['code'])
            for code in codes.to_dict('records'):
                code = code['code']
                if 'stock_daily' in sync_tables:
                    await self.add_task('stock_daily_{}'.format(code), self.sync_stock_daily(code=code))

                if 'stock_index' in sync_tables:
                    await self.add_task('stock_index_{}'.format(code), self.sync_stock_index(code=code))

                if 'stock_fq_factor' in sync_tables:
                    await self.add_task('stock_fq_factor_{}'.format(code), self.sync_stock_fq_factor(code=code))

        if 'stock_margin' in sync_tables:
            codes = await self.stock_db.stock_margin.distinct('code')
            for code in codes:
                await self.add_task('stock_margin_{}'.format(code),
                                    self.sync_stock_margin(code=code))

        if 'stock_index_info' in sync_tables:
            await self.add_task('stock_index_info', self.sync_stock_index_info())

        if 'stock_index_daily' in sync_tables:
            codes = await self.stock_db.load_index_info(projection=['code'])
            for code in codes.to_dict('records'):
                code = code['code']
                if 'stock_index_daily' in sync_tables:
                    await self.add_task('stock_index_daily_{}'.format(code), self.sync_stock_index_daily(code=code))

        if 'stock_ns_flow' in sync_tables:
            await self.add_task('stock_ns_flow', self.sync_stock_ns_flow())

        if 'stock_his_divend' in sync_tables:
            await self.add_task('stock_ns_flow', self.sync_stock_his_divend())

        if 'stock_sw_index_info' in sync_tables:
            await self.add_task('stock_ns_flow', self.sync_sw_index_info())

        if 'stock_concept' in sync_tables:
            await self.add_task('stock_concept', self.sync_stock_concept())

        await self.queue.join()
        self.log.info('同步数据完成')


if __name__ == '__main__':
    from bbq.common import run_until_complete

    a = Mongo2Sql()
    a.init()
    run_until_complete(a.sync(tables='fund_info'))
