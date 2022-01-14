from datetime import datetime, timedelta
from functools import partial
from typing import Dict
import pandas as pd
import click

import bbq.fetch as fetch
from bbq.common import run_until_complete, setup_db, setup_log
from bbq.config import init_def_config
from bbq.data.data_sync import DataSync
from bbq.data.data_sync import Task
from bbq.data.stockdb import StockDB
from bbq.fetch.my_trade_date import is_trade_date


class StockDailyTask(Task):
    def __init__(self, data_sync, name: str, code: str):
        super().__init__(data_sync, name)
        self.code = code

    async def task(self):
        self.log.info('开始同步股票日线数据, code={}'.format(self.code))
        query_func = partial(self.db.load_stock_daily, filter={'code': self.code}, projection=['trade_date'],
                             sort=[('trade_date', -1)], limit=1)
        fetch_func = partial(self.to_async, func=partial(fetch.fetch_stock_daily, code=self.code))
        save_func = self.db.save_stock_daily
        await self.incr_sync_on_trade_date(query_func=query_func,
                                           fetch_func=fetch_func,
                                           save_func=save_func,
                                           sync_start_time_func=lambda now: datetime(year=now.year, month=now.month,
                                                                                     day=now.day, hour=15, minute=30))
        self.log.info('股票日线数据task完成, code={}'.format(self.code))


class StockIndexTask(Task):
    def __init__(self, data_sync, name: str, code: str):
        super().__init__(data_sync, name)
        self.code = code

    async def task(self):
        self.log.info('开始同步股票指标数据, code={}'.format(self.code))
        query_func = partial(self.db.load_stock_index, filter={'code': self.code}, projection=['trade_date'],
                             sort=[('trade_date', -1)], limit=1)
        fetch_func = partial(self.to_async, func=partial(fetch.fetch_stock_index, code=self.code))
        save_func = self.db.save_stock_index
        await self.incr_sync_on_trade_date(query_func=query_func, fetch_func=fetch_func, save_func=save_func)
        self.log.info('股票指标数据task完成, code={}'.format(self.code))


class StockFactorTask(Task):
    def __init__(self, data_sync, name: str, code: str):
        super().__init__(data_sync, name)
        self.code = code

    async def task(self):
        self.log.info('开始获取复权因子数据')

        # data = fetch.fetch_stock_adj_factor(code=self.code)
        # if data is None:
        #     return False
        #
        # data_db = await self.db.load_stock_fq_factor(filter={'code': self.code}, projection=['trade_date'])
        # if data_db is None or data_db.shape[0] != data.shape[0]:
        #     data_new = self.gen_incr_data('trade_date', data_db, data)
        #     if data_new is not None:
        #         self.log.info('删除原有{}复权因子'.format(self.code))
        #         await self.db.do_delete(self.db.stock_fq_factor, filter={'code': self.code}, just_one=False)
        #         await self.db.save_stock_fq_factor(data)

        query_func = partial(self.db.load_stock_fq_factor, filter={'code': self.code}, projection=['sync_date'],
                             sort=[('sync_date', -1)], limit=1)
        fetch_func = partial(self.to_async, func=partial(fetch.fetch_stock_adj_factor, code=self.code))

        async def _save_func(code, data):
            self.log.info('全量同步复权因子: {}'.format(self.code))
            await self.db.do_delete(self.db.stock_fq_factor, filter={'code': code}, just_one=False)
            await self.db.save_stock_fq_factor(data)

        save_func = partial(_save_func, code=self.code)

        is_synced = await self.incr_sync_on_trade_date(cmp_key='sync_date',
                                                       query_func=query_func,
                                                       fetch_func=fetch_func,
                                                       save_func=save_func)

        if not is_synced:
            now = datetime.now()
            now = datetime(year=now.year, month=now.month, day=now.day)
            await self.db.do_update_many(coll=self.db.stock_fq_factor,
                                         filter={'code': self.code},
                                         update={'sync_date': now}, upsert=False)

        self.log.info('获取复权因子数据完成')


class IndexDailyTask(Task):
    def __init__(self, data_sync, name: str, code: str):
        super().__init__(data_sync, name)
        self.code = code

    async def task(self):
        self.log.info('开始同步指数日线数据, code={}'.format(self.code))
        query_func = partial(self.db.load_index_daily, filter={'code': self.code}, projection=['trade_date'],
                             sort=[('trade_date', -1)], limit=1)
        fetch_func = partial(self.to_async, func=partial(fetch.fetch_stock_index_daily, code=self.code))
        save_func = self.db.save_index_daily
        await self.incr_sync_on_trade_date(query_func=query_func, fetch_func=fetch_func, save_func=save_func)
        self.log.info('指数日线数据task完成, code={}'.format(self.code))


class StockNorthFlowTask(Task):
    def __init__(self, data_sync):
        super().__init__(data_sync, 'north_flow')

    async def task(self):
        self.log.info('开始获取北向资金数据')
        query_func = partial(self.db.load_stock_north_south_flow, projection=['trade_date'],
                             sort=[('trade_date', -1)], limit=1)
        filter_cond = None
        now = datetime.now()
        if fetch.is_trade_date(now):
            if now < datetime(year=now.year, month=now.month, day=now.day, hour=15, minute=30):
                end = now - timedelta(days=1)
                filter_cond = 'trade_date <= "{}"'.format(end.strftime('%Y-%m-%d'))
        fetch_func = partial(self.to_async, func=fetch.fetch_stock_north_south_flow)
        save_func = self.db.save_stock_north_south_flow
        await self.incr_sync_on_trade_date(query_func=query_func, fetch_func=fetch_func, save_func=save_func,
                                           filter_data_func=lambda data: data.query(filter_cond) if (
                                                   data is not None and filter_cond is not None) else data)
        self.log.info('获取北向资金数据完成')


class StockHisDivEndTask(Task):
    def __init__(self, data_sync):
        super().__init__(data_sync, 'his_divend')

    async def task(self):
        self.log.info('开始获取历史分红数据')
        # await self.incr_sync_on_code(query_func=partial(self.db.load_stock_his_divend, projection=['code']),
        #                              fetch_func=fetch.fetch_stock_his_divend,
        #                              save_func=self.db.save_stock_his_divend)

        query_func = partial(self.db.load_stock_his_divend, projection=['sync_date'],
                             sort=[('sync_date', -1)], limit=1)

        async def fetch_func(*_1, **_2):
            return fetch.fetch_stock_his_divend()

        async def _save_func(data):
            self.log.info('全量同步历史分红数据')
            await self.db.do_delete(self.db.stock_his_divend, just_one=False)
            await self.db.save_stock_his_divend(data)

        save_func = _save_func

        await self.incr_sync_on_trade_date(cmp_key='sync_date',
                                           query_func=query_func,
                                           fetch_func=fetch_func,
                                           save_func=save_func)

        self.log.info('获取历史分红数据完成')


class SWIndexInfoTask(Task):
    def __init__(self, data_sync):
        super().__init__(data_sync, 'sw_index_info')

    async def task(self):
        self.log.info('开始获取申万一级行业数据')
        await self.incr_sync_on_code(query_func=partial(self.db.load_sw_index_info, projection=['index_code']),
                                     fetch_func=fetch.fetch_stock_sw_index_info,
                                     save_func=self.db.save_sw_index_info, cmp_key='index_code')
        self.log.info('获取获取申万一级行业数据完成')


class StockMarginTask(Task):
    def __init__(self, data_sync, name: str, code: str):
        super().__init__(data_sync, name)
        self.code = code

    async def task(self):
        self.log.info('开始融资融券数据, code={}'.format(self.code))
        # 深圳市场是隔开一个交易日

        trade_date = await self.db.load_stock_margin(filter={'code': self.code}, projection=['trade_date'],
                                                     sort=[('trade_date', -1)], limit=1)
        need_sync = True
        if trade_date is not None:
            need_sync = False
            now = datetime.now()
            now = datetime(year=now.year, month=now.month, day=now.day)

            last_trade_date = now
            while not is_trade_date(last_trade_date):
                last_trade_date = last_trade_date + timedelta(days=-1)

            sync_start_date = trade_date['trade_date'].iloc[0] + timedelta(days=1)
            while not is_trade_date(sync_start_date):
                sync_start_date = sync_start_date + timedelta(days=1)

            if self.code[:2] == 'sh':
                delta = last_trade_date - sync_start_date
                if delta.days >= 1:
                    need_sync = True
            if self.code[:2] == 'sz':
                delta = last_trade_date - sync_start_date
                if delta.days >= 2:
                    need_sync = True
        if not need_sync:
            return

        async def query_func():
            return trade_date

        fetch_func = partial(self.to_async, func=partial(fetch.fetch_stock_margin, code=self.code))
        save_func = self.db.save_stock_margin
        await self.incr_sync_on_trade_date(query_func=query_func,
                                           fetch_func=fetch_func,
                                           save_func=save_func,
                                           sync_start_time_func=lambda now: datetime(year=now.year, month=now.month,
                                                                                     day=now.day, hour=15, minute=30))
        self.log.info('股票融资融券数据task完成, code={}'.format(self.code))


class StockConceptTask(Task):
    def __init__(self, data_sync, name: str):
        super().__init__(data_sync, name)

    async def task(self):
        self.log.info('开始获取股票概念数据')
        start = await self.db.load_stock_concept(projection=['concept_date'],
                                                 limit=1, sort=[('concept_date', -1)])
        start = None if start is None or start.empty else start.iloc[0]['concept_date']

        async def query_func():
            data = await self.db.stock_concept.distinct('concept_code')
            if data is None or len(data) == 0:
                return None
            return pd.DataFrame([{'concept_code': code} for code in data])
        await self.incr_sync_on_code(query_func=query_func,
                                     fetch_func=partial(fetch.fetch_stock_concept, start=start),
                                     save_func=self.db.save_stock_concept, cmp_key='concept_code')
        self.log.info('获取获取股票概念数据完成')


class StockSync(DataSync):
    def __init__(self, db: StockDB, config: Dict):
        super().__init__(db=db,
                         concurrent_fetch_count=config['con_fetch_num'],
                         concurrent_save_count=config['con_save_num'])
        self.config = config
        self.funcs = self.config['function'].split(',') if self.config['function'] is not None else None

    async def post_tasks(self) -> bool:
        """
        融资融券要容易封IP，和查询日K数据，特殊处理
        """
        return True

    async def prepare_tasks(self) -> bool:
        codes = None
        indexes = None
        if not self.config['skip_basic']:
            codes = await self.incr_sync_on_code(query_func=partial(self.db.load_stock_info, projection=['code']),
                                                 fetch_func=fetch.fetch_stock_info,
                                                 save_func=self.db.save_stock_info)

            indexes = await self.incr_sync_on_code(query_func=partial(self.db.load_index_info, projection=['code']),
                                                   fetch_func=fetch.fetch_index_info,
                                                   save_func=self.db.save_index_info)
        else:
            codes = await self.db.load_stock_info(projection=['code'])
            indexes = await self.db.load_index_info(projection=['code'])

        if codes is None or indexes is None:
            self.log.error('股票信息和指数信息为空, 请求先同步基础数据...')
            return False

        self.log.info('开始准备task...')
        if self.funcs is None or 'stock_daily' in self.funcs:
            for _, item in codes.iterrows():
                self.add_task(
                    StockDailyTask(data_sync=self, name='stack_daily_{}'.format(item['code']), code=item['code']))

        if self.funcs is None or 'stock_index' in self.funcs:
            for _, item in codes.iterrows():
                self.add_task(StockIndexTask(self, name='stack_index_{}'.format(item['code']), code=item['code']))

        if self.funcs is None or 'index_daily' in self.funcs:
            for _, item in indexes.iterrows():
                self.add_task(IndexDailyTask(self, name='index_daily_{}'.format(item['code']), code=item['code']))

        if self.funcs is None or 'stock_fq_factor' in self.funcs:
            for _, item in codes.iterrows():
                self.add_task(StockFactorTask(self, name='stock_fq_factor_{}'.format(item['code']), code=item['code']))

        if self.funcs is None or 'stock_north_flow' in self.funcs:
            self.add_task(StockNorthFlowTask(self))

        if self.funcs is None or 'stock_his_divend' in self.funcs:
            self.add_task(StockHisDivEndTask(self))

        if self.funcs is None or 'stock_margin' in self.funcs:
            margin_codes = await self.db.load_stock_info(filter={'is_margin': 1}, projection=['code'])
            for _, item in margin_codes.iterrows():
                self.add_task(
                    StockMarginTask(self, name='stock_margin_{}'.format(item['code']), code=item['code']))

        if self.funcs is None or 'stock_concept' in self.funcs:
            self.add_task(StockConceptTask(self, name='stock_concept'))

        # 没有用
        if self.funcs is not None and 'sw_index_info' in self.funcs:
            self.add_task(SWIndexInfoTask(self))

        self.log.info('task count={}'.format(len(self.tasks)))

        return True


@click.command()
@click.option('--uri', type=str, default='mongodb://localhost:27017/', help='mongodb connection uri')
@click.option('--pool', default=10, type=int, help='mongodb connection pool size')
@click.option('--skip-basic/--no-skip-basic', default=False, type=bool, help='skip sync basic')
@click.option('--con-fetch-num', default=10, type=int, help='concurrent net fetch number')
@click.option('--con-save-num', default=100, type=int, help='concurrent db save number')
@click.option('--function', type=str,
              help='sync one, split by ",", available: stock_daily,stock_index,index_daily,stock_fq_factor,'
                   'stock_north_flow,stock_his_divend,sw_index_info,stock_margin,stock_concept')
@click.option('--debug/--no-debug', default=True, type=bool, help='show debug log')
def main(uri: str = 'mongodb://localhost:27017/', pool: int = 5,
         skip_basic: bool = False,
         con_fetch_num: int = 10, con_save_num: int = 100,
         function: str = None, debug: bool = True):
    _, conf_dict = init_def_config()
    conf_dict['mongo'].update(dict(uri=uri, pool=pool))
    conf_dict['log'].update(dict(level="debug" if debug else "critical"))
    setup_log(conf_dict, 'stock_sync.log')
    db = setup_db(conf_dict, StockDB)
    if db is None:
        return
    config = dict(skip_basic=skip_basic,
                  con_fetch_num=con_fetch_num,
                  con_save_num=con_save_num,
                  function=function)
    sync = StockSync(db=db, config=config)
    run_until_complete(sync.sync())


if __name__ == '__main__':
    main()
