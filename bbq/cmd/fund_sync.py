from bbq import log
import asyncio
from datetime import datetime, timedelta
import traceback
import click
from bbq.config import conf_dict
import os
from bbq.common import run_until_complete
from bbq.data.funddb import FundDB
from bbq.fetch.fund_eastmoney import FundEastmoney


class FundSync:
    concurrent_count = 80

    def __init__(self, db, fund):
        self.log = log.get_logger(FundSync.__name__)
        self.db = db
        self.fund = fund

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
        return False if typ in ['QDII', 'QDII-ETF', 'QDII-指数', '债券型', '债券指数', '定开债券',
                                '理财型', '货币型'] else True

    async def sync_task(self, code, queue):
        try:
            self.log.debug('增量同步基金信息: {}'.format(code))
            fund_info = await self.fund.get_fund_info(code=code)
            await self.db.save_fund_info(fund_info)

            net_info = await self.db.load_fund_net(code=code, projection=['code', 'date'], sort=[('date', -1)],
                                                   limit=1)
            if net_info is None:
                if net_info is not None:
                    net_info['short_name'] = fund_info['short_name'][0]
                    await self.db.save_fund_net(code=code, data=net_info)
            else:
                last = net_info.loc[0, 'date']
                now = datetime.now()

                delta = now - last
                if delta.days > 1:
                    start_date = datetime.strptime(last.strftime('%Y%m%d'), '%Y%m%d') + timedelta(days=1)
                    end_date = datetime.strptime(now.strftime('%Y%m%d'), '%Y%m%d')
                    net_info = await self.fund.get_fund_net(code=code, start_date=start_date, end_date=end_date)
                    if net_info is not None:
                        net_info['short_name'] = fund_info['short_name'][0]
                        await self.db.save_fund_net(code=code, data=net_info)
        except Exception as e:
            self.log.error('同步基金信息失败: code={} ex={} stack={}'.format(code, e, traceback.format_exc()))
        finally:
            await queue.get()
            queue.task_done()

    async def sync(self):
        try:
            self.log.info('获取基金列表...')
            funds = await self.fund.get_fund_list(fields='code,type')

            queue = asyncio.Queue(self.concurrent_count)
            loop = asyncio.get_event_loop()
            for _, fund in funds.iterrows():
                code, typ = fund['code'], fund['type']
                if not self.filter_type(typ):
                    self.log.debug('忽略基金类型: {}, {}'.format(code, typ))
                    continue
                await queue.put(code)
                coro = self.sync_task(code=fund['code'], queue=queue)
                loop.create_task(coro)
            await queue.join()

            self.log.info('同步板块列表...')
            funds = await self.fund.get_block_list(sync_fund=True)
            await self.db.save_block_list(funds)

            # await self.db.build_index()
            self.log.info('基金信息同步完成')
        except Exception as e:
            self.log.error('同步基金失败: ex={}, stack={}'.format(e, traceback.format_exc()))


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
        file = conf_dict['log']['path'] + os.sep + 'fund_sync.log'
        level = conf_dict['log']['level']

    log.setup_logger(file=file, level=level)
    logger = log.get_logger()
    logger.debug('初始化数据库')
    db = FundDB(uri=uri, pool=pool)
    if not db.init():
        print('初始化数据库失败')
        return

    fund = FundEastmoney()
    fund_sync = FundSync(db, fund)
    run_until_complete(fund_sync.sync())


if __name__ == '__main__':
    main()
