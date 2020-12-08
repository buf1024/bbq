from bbq.trade.base_obj import BaseObj


class Entrust(BaseObj):
    def __init__(self, entrust_id: str, account):
        super().__init__(typ=account.typ, db_data=account.db_data, db_trade=account.db_trade, trader=account.trader)
        self.account = account

        self.entrust_id = entrust_id

        self.name = ''  # 股票名称
        self.code = ''  # 股票代码
        self.time = None  # 委托时间

        self.broker_entrust_id = ''  # broker对应的委托id
        self.type = None  # buy, sell, cancel
        self.status = 'commit'  # commit 已提交 deal 已成 part_deal 部成 cancel 已经取消

        self.price = 0.0  # 价格
        self.volume = 0  # 量

        self.volume_deal = 0  # 已成量
        self.volume_cancel = 0  # 已取消量

    async def sync_from_db(self) -> bool:
        entrust = await self.db_trade.load_entrust(filter={'entrust_id': self.entrust_id}, limit=1)
        entrust = None if len(entrust) == 0 else entrust[0]
        if entrust is None:
            self.log.error('entrust from db not found: {}'.format(self.entrust_id))
            return False
        self.name = entrust['name']
        self.code = entrust['code']
        self.volume_deal = entrust['volume_deal']
        self.volume_cancel = entrust['volume_cancel']
        self.volume = entrust['volume']
        self.price = entrust['price']
        self.status = entrust['status']
        self.type = entrust['type']
        self.broker_entrust_id = entrust['broker_entrust_id']
        self.time = entrust['time']
        return True

    @BaseObj.discard_saver
    async def sync_to_db(self) -> bool:
        data = {'account_id': self.account.account_id,
                'entrust_id': self.entrust_id, 'name': self.name, 'code': self.code,
                'volume_deal': self.volume_deal, 'volume_cancel': self.volume_cancel,
                'volume': self.volume, 'price': self.price,
                'status': self.status,
                'type': self.type, 'broker_entrust_id': self.broker_entrust_id,
                'time': self.time
                }
        await self.db_trade.save_entrust(data=data)
        return True
