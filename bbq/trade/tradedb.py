from bbq.data.mongodb import MongoDB
from typing import List, Dict


class TradeDB(MongoDB):
    _meta = {
        # 账户信息
        'account_info': {'account_id': '账户id', 'status': '账户状态(0正常 其他停止)',
                         'kind': '交易种类: stock, fund', 'type': '账户类型: real, simulate, backtest',
                         'strategy_id': '交易策略id', 'broker_id': '券商id', 'risk_id': '风控id',
                         'cash_init': '初始资金', 'cash_available': '可用资金', 'cost': '持仓成本',
                         'broker_fee': '手续费', "transfer_fee": '过户费', "tax_fee": '印花税',
                         'profit': '盈利', 'profit_rate': '盈利比例', 'start_time': '开始时间', 'end_time': '结束时间'},
        # 策略信号
        'signal_info': {'account_id': '账户id', 'signal_id': '信号id', 'source': '信号源, risk, strategy, broker, manual',
                        'signal': '信号: sell, buy, cancel', 'name': '股票名称', 'code': '股票代码',
                        'price': '价', 'volume': '量', 'entrust_id': '委托ID', 'time': '信号时间'},

        # 委托信息
        'entrust_info': {'account_id': '账户id', 'entrust_id': '委托ID', 'name': '股票名称', 'code': '股票代码',
                         'broker_entrust_id': 'broker对应的委托id', 'broker_id': '对应的broker',
                         'type': '委托类型: buy, sell, cancel',
                         'status': '委托状态: commit 已提交 deal 已成 part_deal 部成 cancel 已取消',
                         'price': '价', 'volume': '量', 'volume_deal': '已成量', 'volume_cancel': '已取消量',
                         'time': '委托时间'},

        # 成交历史
        'deal_info': {'account_id': '账户id', 'deal_id': '成交ID', 'entrust_id': '委托ID',
                      'name': '股票名称', 'code': '股票代码', 'price': '价', 'volume': '量',
                      'time': '成交时间'},

        # 持仓信息
        'position_info': {'account_id': '账户id', 'position_id': '持仓ID',  'name': '股票名称', 'code': '股票代码',
                          'volume': '持仓量', 'volume_available': '可用持仓量', 'cost': '平均持仓成本', 'price': '平均持仓价',
                          'profit_rate': '盈利比例', 'max_profit_rate': '最大盈利比例', 'min_profit_rate': '最小盈利比例',
                          'profit': '盈利比例', 'max_profit': '最大盈利', 'min_profit': '最小盈利',
                          'now_price': '最新价', 'max_price': '最高价', 'min_price': '最低价',
                          'max_profit_time': '最大盈利时间', 'min_profit_time': '最小盈利时间',
                          'time': '首次建仓时间'},

        # 策略相关信息
        'strategy_info': {'account_id': '账户id',
                          'strategy_id': '策略名称id', 'strategy_opt': '策略参数',
                          'broker_id': '券商名称id', 'broker_opt': '券商参数',
                          'risk_id': '风控名称id', 'risk_opt': '风控参数'}

    }

    _db = 'bbq_trade_db'  # 交易数据库

    def __init__(self, uri='mongodb://localhost:27017/', pool=5):
        super().__init__(uri, pool)

    @property
    def account_info(self):
        return self.get_coll(self._db, 'account_info')

    @property
    def signal_info(self):
        return self.get_coll(self._db, 'signal_info')

    @property
    def entrust_info(self):
        return self.get_coll(self._db, 'entrust_info')

    @property
    def deal_info(self):
        return self.get_coll(self._db, 'deal_info')

    @property
    def position_info(self):
        return self.get_coll(self._db, 'position_info')

    @property
    def strategy_info(self):
        return self.get_coll(self._db, 'strategy_info')

    async def load_account(self, **kwargs) -> List:
        self.log.debug('查询账户, kwargs={} ...'.format(kwargs))
        data = await self.do_load(self.account_info, to_frame=False, **kwargs)
        self.log.debug('查询账户成功 data={}'.format(data))
        return data

    async def save_account(self, data: Dict):
        self.log.debug('保存账户信息, data = {}'.format(data))
        inserted_ids = await self.do_update(coll=self.account_info,
                                            filter={'account_id': data['account_id']}, update=data)
        self.log.debug('保存账户信息成功')
        return inserted_ids

    async def load_signal(self, **kwargs) -> List:
        self.log.debug('查询信号信息, kwargs={} ...'.format(kwargs))
        data = await self.do_load(self.signal_info, to_frame=False, **kwargs)
        self.log.debug('查询信号信息成功 data={}'.format(data))
        return data

    async def save_signal(self, data: Dict):
        self.log.debug('保存信号信息, data = {}'.format(data))
        inserted_ids = await self.do_update(coll=self.signal_info,
                                            filter={'signal_id': data['signal_id']}, update=data)
        self.log.debug('保存信号信息成功')
        return inserted_ids

    async def load_entrust(self, **kwargs) -> List:
        self.log.debug('查询委托信息, kwargs={} ...'.format(kwargs))
        data = await self.do_load(self.entrust_info, to_frame=False, **kwargs)
        self.log.debug('查询委托信息成功 data={}'.format(data))
        return data

    async def save_entrust(self, data: Dict):
        self.log.debug('保存信号信息, data = {}'.format(data))
        inserted_ids = await self.do_update(coll=self.entrust_info,
                                            filter={'entrust_id': data['entrust_id']}, update=data)
        self.log.debug('保存信号信息成功')
        return inserted_ids

    async def load_deal(self, **kwargs) -> List:
        self.log.debug('查询成交历史, kwargs={} ...'.format(kwargs))
        data = await self.do_load(self.deal_info, to_frame=False, **kwargs)
        self.log.debug('查询成交历史成功 data={}'.format(data))
        return data

    async def save_deal(self, data: Dict):
        self.log.debug('保存成交历史, data = {}'.format(data))
        inserted_ids = await self.do_update(coll=self.deal_info,
                                            filter={'deal_id': data['deal_id']}, update=data)
        self.log.debug('保存成交历史成功')
        return inserted_ids

    async def load_position(self, **kwargs) -> List:
        self.log.debug('查询持仓信息, kwargs={} ...'.format(kwargs))
        data = await self.do_load(self.position_info, to_frame=False, **kwargs)
        self.log.debug('查询持仓信息成功 data={}'.format(data))
        return data

    async def save_position(self, data: Dict):
        self.log.debug('保存持仓信息, data = {}'.format(data))
        inserted_ids = await self.do_update(coll=self.position_info,
                                            filter={'signal_id': data['signal_id']}, update=data)
        self.log.debug('保存持仓信息成功')
        return inserted_ids

    async def load_strategy(self, **kwargs) -> List:
        self.log.debug('查询策略信息, kwargs={} ...'.format(kwargs))
        data = await self.do_load(self.strategy_info, to_frame=False, **kwargs)
        self.log.debug('查询策略信息成功 data={}'.format(data))
        return data

    async def save_strategy(self, data: Dict):
        self.log.debug('保存策略信息, data = {}'.format(data))
        inserted_ids = await self.do_update(coll=self.strategy_info,
                                            filter={'account_id': data['account_id']}, update=data)
        self.log.debug('保存策略信息成功')
        return inserted_ids


if __name__ == '__main__':
    import uuid
    from bbq.common import run_until_complete
    acct1 = dict(account_id=str(uuid.uuid4()), status=0, kind='fund', type='stock', data=123)
    acct2 = dict(account_id=str(uuid.uuid4()), status=0, kind='fund', type='stock', data=456)

    db = TradeDB()
    db.init()

    async def test_save():
        await db.save_account(data=acct1)
        await db.save_account(data=acct2)

    async def test_load():
        data = await db.load_account(filter=dict(status=0, kind='fund', type='stock'), limit=1)
        print(data)

    run_until_complete(
        test_load()
    )
