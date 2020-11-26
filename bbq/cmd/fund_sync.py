from functools import partial
import asyncio
from datetime import datetime, timedelta
import traceback
import click
from bbq.common import run_until_complete
from bbq.data.funddb import FundDB
from bbq.common import setup_log, setup_db
from bbq.data.data_sync import DataSync, Task
from typing import Dict
from bbq.fetch.fund_eastmoney import FundEastmoney


class FundInfoTask(Task):
    def __init__(self, ctx, name: str, funds):
        super().__init__(ctx, name)
        self.funds = funds

    async def task(self):
        self.log.info('开始基金基本信息同步任务'.format(self.name, self.code))
        for _, fund in self.funds.iterrows():
            code, typ, name = fund['code'], fund['type'], fund['name']
            if not self.ctx.filter_type(typ):
                # self.log.debug('忽略基金类型: {}, {}'.format(code, typ))
                continue

            fund_info = await self.ctx.fetch.fetch_fund_info(code=code)
            if fund_info is None:
                self.log.error('获取基金{}({})信息失败'.format(name, code))
                continue
            save_func = partial(self.db.save_fund_info, data=fund_info)
            await self.ctx.submit_db(save_func)
        self.log.info('基金基本信息task完成'.format(self.name))


class FundNetTask(Task):
    def __init__(self, ctx, name: str, code: str):
        super().__init__(ctx, name)
        self.code = code

    async def task(self):
        self.log.info('开始基金净值{}({})同步任务'.format(self.name, self.code))
        query_func = partial(self.ctx.db.load_fund_net, filter={'code': self.code}, projection=['date'],
                             sort=[('date', -1)], limit=1)
        fetch_func = partial(self.ctx.fetch.fetch_fund_net, code=self.code)
        save_func = self.db.save_fund_net
        await self.incr_sync_on_trade_date(query_func=query_func, fetch_func=fetch_func, save_func=save_func,
                                           key='date',
                                           sync_start_time_func=lambda now: datetime(year=now.year, month=now.month,
                                                                                     day=now.day, hour=0, minute=0))
        self.log.info('基金净值{}({})同步任务完成'.format(self.name, self.code))


class FundBlockTask(Task):
    def __init__(self, ctx, name: str):
        super().__init__(ctx, name)

    async def task(self):
        self.log.info('同步基金板块列表...')
        funds = await self.ctx.fetch.fetch_block_list(sync_fund=True)
        save_func = partial(self.db.save_block_list, data=funds)
        await self.ctx.submit_db(save_func)
        self.log.info('基金板块数据task完成')


class FundSync(DataSync):
    def __init__(self, fetch: FundEastmoney, db: FundDB, config: Dict):
        super().__init__(db=db,
                         concurrent_fetch_count=config['con_fetch_num'],
                         concurrent_save_count=config['con_save_num'])
        self.config = config
        self.funcs = self.config['function'].split(',') if self.config['function'] is not None else None
        self.fetch = fetch

    @staticmethod
    def filter_type(typ):
        # "ETF-场内",
        # "QDII",
        # "QDII-ETF",
        # "QDII-指数",
        # "债券型",
        # "债券指数",
        # "分级杠杆",
        # "固定收益",
        # "定开债券",
        # "混合-FOF",
        # "混合型",
        # "理财型",
        # "联接基金",
        # "股票型",
        # "股票指数",
        # "货币型"
        # return False if typ in ['QDII', 'QDII-ETF', 'QDII-指数', '债券型', '债券指数', '固定收益', '定开债券',
        #                         '理财型', '货币型'] else True
        return False if typ in ['QDII', 'QDII-ETF', 'QDII-指数', '债券型', '债券指数', '定开债券',
                                '理财型', '货币型'] else True

    async def prepare_tasks(self) -> bool:
        self.log.info('获取基金列表...')
        funds = await self.fetch.fetch_fund_list(fields='code,name,type')
        if funds is None:
            self.log.error('获取基金列表信息失败')
            return False

        if self.funcs is None or 'fund_info' in self.funcs:
            self.add_task(FundInfoTask(self, name='fund_info', funds=funds))

        if self.funcs is None or 'fund_net' in self.funcs:
            for _, fund in funds.iterrows():
                code, typ, name = fund['code'], fund['type'], fund['name']
                if not self.filter_type(typ):
                    # self.log.debug('忽略基金类型: {}, {}'.format(code, typ))
                    continue
                self.add_task(FundNetTask(self, name=name, code=code))

        if self.funcs is None or 'fund_block' in self.funcs:
            self.add_task(FundBlockTask(self, name='fund_block'))

        return True


@click.command()
@click.option('--uri', type=str, help='mongodb connection uri')
@click.option('--pool', default=0, type=int, help='mongodb connection pool size')
@click.option('--con-fetch-num', default=20, type=int, help='concurrent net fetch number')
@click.option('--con-save-num', default=100, type=int, help='concurrent db save number')
@click.option('--function', type=str,
              help='sync one, split by ",", available: stock_daily,stock_index,index_daily,stock_qf_factor,'
                   'stock_north_flow,stock_his_divend,sw_index_info')
@click.option('--debug/--no-debug', default=True, help='show debug log')
def main(uri: str, pool: int, con_fetch_num: int, con_save_num: int, function: str, debug: bool):
    setup_log(debug, 'fund_sync.log')
    db = setup_db(uri, pool, FundDB)
    if db is None:
        return
    fund = FundEastmoney()
    config = dict(con_fetch_num=con_fetch_num,
                  con_save_num=con_save_num,
                  function=function)
    fund_sync = FundSync(fetch=fund, db=db, config=config)
    run_until_complete(fund_sync.sync())


if __name__ == '__main__':
    main()
