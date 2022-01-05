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
    def __init__(self, data_sync, name: str, fund_info):
        super().__init__(data_sync, name)
        self.fund_info = fund_info

    async def task(self):
        self.log.info('开始基金基本信息同步任务: {}'.format(self.name))
        save_func = partial(self.db.save_fund_info, data=self.fund_info)
        await self.data_sync.submit_db(save_func)

        self.log.info('基金基本信息{}, task完成'.format(self.name))


class FundDailyTask(Task):
    def __init__(self, data_sync, name: str, code: str):
        super().__init__(data_sync, name)
        self.code = code

    async def task(self):
        self.log.info('开始场内基金日线{}({})同步任务'.format(self.name, self.code))
        query_func = partial(self.db.load_fund_daily, filter={'code': self.code}, projection=['trade_date'],
                             sort=[('trade_date', -1)], limit=1)
        fetch_func = partial(self.to_async,
                             func=partial(fetch.fetch_fund_daily,
                                          code=self.code, start=datetime(year=1990, month=1, day=1)))
        save_func = self.db.save_fund_daily
        await self.incr_sync_on_trade_date(query_func=query_func, fetch_func=fetch_func, save_func=save_func,
                                           sync_start_time_func=lambda now: datetime(year=now.year,
                                                                                     month=now.month,
                                                                                     day=now.day, hour=15,
                                                                                     minute=30))
        self.log.info('开始场内基金日线{}({})同步任务完成'.format(self.name, self.code))


class FundNetTask(Task):
    def __init__(self, data_sync, name: str, code: str):
        super().__init__(data_sync, name)
        self.code = code

    async def task(self):
        self.log.info('开始基金净值{}({})同步任务'.format(self.name, self.code))
        query_func = partial(self.db.load_fund_net, filter={'code': self.code}, projection=['trade_date'],
                             sort=[('trade_date', -1)], limit=1)
        fetch_func = partial(self.to_async, partial(fetch.fetch_fund_net, code=self.code))
        save_func = self.db.save_fund_net
        await self.incr_sync_on_trade_date(query_func=query_func, fetch_func=fetch_func, save_func=save_func,
                                           sync_start_time_func=lambda now: datetime(year=now.year, month=now.month,
                                                                                     day=now.day, hour=0, minute=0))
        self.log.info('基金净值{}({})同步任务完成'.format(self.name, self.code))


class FundSync(DataSync):
    def __init__(self, db: FundDB, config: Dict):
        super().__init__(db=db,
                         concurrent_fetch_count=config['con_fetch_num'],
                         concurrent_save_count=config['con_save_num'])
        self.config = config
        self.funcs = self.config['function'].split(',') if self.config['function'] is not None else None

    async def prepare_tasks(self) -> bool:
        self.log.info('获取基金列表...')
        funds = fetch.fetch_fund_info(types=['ETF-场内'])
        if funds is None:
            self.log.error('获取基金列表信息失败')
            return False

        if self.funcs is None or 'fund_info' in self.funcs:
            self.add_task(FundInfoTask(data_sync=self, name='fund_info', fund_info=funds))

        if self.funcs is None or 'fund_daily' in self.funcs:
            for _, fund in funds.iterrows():
                self.add_task(FundDailyTask(data_sync=self, name=fund['name'], code=fund['code']))

        if self.funcs is None or 'fund_net' in self.funcs:
            for _, fund in funds.iterrows():
                self.add_task(FundNetTask(data_sync=self, name=fund['name'], code=fund['code']))

        return True


@click.command()
@click.option('--uri', type=str, default='mongodb://localhost:27017/', help='mongodb connection uri')
@click.option('--pool', default=10, type=int, help='mongodb connection pool size')
@click.option('--con-fetch-num', default=20, type=int, help='concurrent net fetch number')
@click.option('--con-save-num', default=100, type=int, help='concurrent db save number')
@click.option('--function', type=str,
              help='sync one, split by ",", available: fund_info,fund_net,fund_daily')
@click.option('--debug/--no-debug', default=True, help='show debug log')
def main(uri: str = 'mongodb://localhost:27017/', pool: int = 5,
         con_fetch_num: int = 10, con_save_num: int = 100,
         function: str = None, debug: bool = True):
    _, conf_dict = init_def_config()
    conf_dict['mongo'].update(dict(uri=uri, pool=pool))
    conf_dict['log'].update(dict(level="debug" if debug else "critical"))
    setup_log(conf_dict, 'fund_sync.log')
    db = setup_db(conf_dict, FundDB)
    if db is None:
        return
    config = dict(con_fetch_num=con_fetch_num,
                  con_save_num=con_save_num,
                  function=function)
    fund_sync = FundSync(db=db, config=config)
    run_until_complete(fund_sync.sync())


if __name__ == '__main__':
    main()
