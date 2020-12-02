from functools import partial
from datetime import datetime
import click
from bbq.common import run_until_complete
from bbq.data.funddb import FundDB
from bbq.common import setup_log, setup_db
from bbq.data.data_sync import DataSync, Task
from typing import Dict
import bbq.fetch as fetch
from bbq.config import init_def_config


class FundInfoTask(Task):
    def __init__(self, ctx, name: str, code):
        super().__init__(ctx, name)
        self.code = code

    async def task(self):
        self.log.info('开始基金基本信息同步任务: {}({})'.format(self.name, self.code))
        fund_info = await fetch.fetch_fund_info(code=self.code)
        if fund_info is None:
            self.log.error('获取基金{}({})信息失败'.format(self.name, self.code))
            return
        save_func = partial(self.db.save_fund_info, data=fund_info)
        await self.ctx.submit_db(save_func)

        self.log.info('基金基本信息{}({}), task完成'.format(self.name, self.code))


class FundDailyTask(Task):
    def __init__(self, ctx, name: str, code: str):
        super().__init__(ctx, name)
        self.code = code

    async def task(self):
        self.log.info('开始场内基金日线{}({})同步任务'.format(self.name, self.code))
        fund_info = await self.ctx.db.load_fund_info(filter={'code': self.code}, limit=1)
        if fund_info is None:
            self.log.info('场内基金日线{}({})基础信息尚未同步，下次增量同步'.format(self.name, self.code))
            return

        query_func = partial(self.ctx.db.load_fund_daily, filter={'code': self.code}, projection=['trade_date'],
                             sort=[('trade_date', -1)], limit=1)
        fetch_func = partial(self.fetch_daily, fund_info=fund_info, code=self.code)
        save_func = self.db.save_fund_daily
        await self.incr_sync_on_trade_date(query_func=query_func, fetch_func=fetch_func, save_func=save_func,
                                           sync_start_time_func=lambda now: datetime(year=now.year,
                                                                                     month=now.month,
                                                                                     day=now.day, hour=15,
                                                                                     minute=30))
        self.log.info('开始场内基金日线{}({})同步任务完成'.format(self.name, self.code))

    async def fetch_daily(self, fund_info, code, start, end):
        if start is None:
            issue_date = fund_info['issue_date'].iloc[0]
            start = issue_date
            if start.year == 1900:
                start = fund_info['found_date'].iloc[0]

        if start.year == 1900:
            self.log.error('基金日线{}无发行信息，暂不同步'.format(code))
            return None

        return fetch.fetch_fund_daily(code=code, start=start, end=end)


class FundNetTask(Task):
    def __init__(self, ctx, name: str, code: str):
        super().__init__(ctx, name)
        self.code = code

    async def task(self):
        self.log.info('开始基金净值{}({})同步任务'.format(self.name, self.code))
        query_func = partial(self.ctx.db.load_fund_net, filter={'code': self.code}, projection=['trade_date'],
                             sort=[('trade_date', -1)], limit=1)
        fetch_func = partial(fetch.fetch_fund_net, code=self.code)
        save_func = self.db.save_fund_net
        await self.incr_sync_on_trade_date(query_func=query_func, fetch_func=fetch_func, save_func=save_func,
                                           sync_start_time_func=lambda now: datetime(year=now.year, month=now.month,
                                                                                     day=now.day, hour=0, minute=0))
        self.log.info('基金净值{}({})同步任务完成'.format(self.name, self.code))


class FundBlockTask(Task):
    def __init__(self, ctx, name: str):
        super().__init__(ctx, name)

    async def task(self):
        self.log.info('同步基金板块列表...')
        funds = await fetch.fetch_fund_block_list(sync_fund=True)
        save_func = partial(self.db.save_block_list, data=funds)
        await self.ctx.submit_db(save_func)
        self.log.info('基金板块数据task完成')


class FundSync(DataSync):
    def __init__(self, db: FundDB, config: Dict):
        super().__init__(db=db,
                         concurrent_fetch_count=config['con_fetch_num'],
                         concurrent_save_count=config['con_save_num'])
        self.config = config
        self.funcs = self.config['function'].split(',') if self.config['function'] is not None else None

        self.log.debug('sync only etf: {}'.format(self.config['etf_only']))

    def filter_type(self, typ):
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
        if not self.config['etf_only']:
            return False if typ in ['QDII', 'QDII-ETF', 'QDII-指数', '债券型', '债券指数', '定开债券',
                                    '理财型', '货币型'] else True
        return True if typ in ['ETF-场内'] else False

    async def prepare_tasks(self) -> bool:
        self.log.info('获取基金列表...')
        funds = await fetch.fetch_fund_list(fields='code,name,type')
        if funds is None:
            self.log.error('获取基金列表信息失败')
            return False

        if self.funcs is None or 'fund_info' in self.funcs:
            for _, fund in funds.iterrows():
                code, typ, name = fund['code'], fund['type'], fund['name']
                if not self.ctx.filter_type(typ):
                    # self.log.debug('忽略基金类型: {}, {}'.format(code, typ))
                    continue
                self.add_task(FundInfoTask(self, name=name, code=code))

        if self.funcs is None or 'fund_daily' in self.funcs:
            for _, fund in funds.iterrows():
                code, typ, name = fund['code'], fund['type'], fund['name']
                if typ == 'ETF-场内':
                    self.add_task(FundDailyTask(self, name=name, code=code))

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
@click.option('--uri', type=str, default='mongodb://localhost:27017/', help='mongodb connection uri')
@click.option('--pool', default=10, type=int, help='mongodb connection pool size')
@click.option('--con-fetch-num', default=20, type=int, help='concurrent net fetch number')
@click.option('--con-save-num', default=100, type=int, help='concurrent db save number')
@click.option('--function', type=str,
              help='sync one, split by ",", available: fund_info,fund_net,fund_block,fund_daily')
@click.option('--etf-only/--all', default=True, help='only sync trade etf')
@click.option('--debug/--no-debug', default=True, help='show debug log')
def main(uri: str, pool: int, con_fetch_num: int, con_save_num: int, function: str, etf_only: bool, debug: bool):
    _, conf_dict = init_def_config()
    conf_dict['mongo'].update(dict(uri=uri, pool=pool))
    conf_dict['log'].update(dict(level="debug" if debug else "critical"))
    setup_log(conf_dict, 'fund_sync.log')
    db = setup_db(conf_dict, FundDB)
    if db is None:
        return
    config = dict(con_fetch_num=con_fetch_num,
                  con_save_num=con_save_num,
                  function=function,
                  etf_only=etf_only)
    fund_sync = FundSync(db=db, config=config)
    run_until_complete(fund_sync.sync())


if __name__ == '__main__':
    main()
