from bbq.trade.base_obj import BaseObj
from datetime import datetime


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
        self.status = 'init'  # init, 初始化 commit 已提交 deal 已成 part_deal 部成 cancel 已经取消

        self.price = 0.0  # 价格
        self.volume = 0  # 量

        self.volume_deal = 0  # 已成量
        self.volume_cancel = 0  # 已取消量

        self.signal = None

    async def sync_from_db(self) -> bool:
        entrust = await self.db_trade.load_entrust(filter={'entrust_id': self.entrust_id}, limit=1)
        entrust = None if len(entrust) == 0 else entrust[0]
        if entrust is None:
            self.log.error('entrust from db not found: {}'.format(self.entrust_id))
            return False
        self.from_dict(entrust)
        return True

    @BaseObj.discard_saver
    async def sync_to_db(self) -> bool:
        data = {'account_id': self.account.account_id,
                'entrust_id': self.entrust_id,
                'broker_entrust_id': self.broker_entrust_id,
                'name': self.name,
                'code': self.code,
                'type': self.type,
                'price': self.price,
                'volume': self.volume,
                'volume_deal': self.volume_deal,
                'volume_cancel': self.volume_cancel,
                'status': self.status,
                'time': self.time}
        await self.db_trade.save_entrust(data=data)
        return True

    def from_dict(self, data):
        self.name = data['name']
        self.code = data['code']
        self.volume_deal = data['volume_deal']
        self.volume_cancel = data['volume_cancel']
        self.volume = data['volume']
        self.price = data['price']
        self.status = data['status']
        self.type = data['type']
        self.broker_entrust_id = data['broker_entrust_id']
        self.time = data['time'] if isinstance(data['time'], datetime) else datetime.strptime(data['time'],
                                                                                              '%Y-%m-%d %H:%M:%S')

    def to_dict(self):
        return {'account_id': self.account.account_id,
                'entrust_id': self.entrust_id,
                'broker_entrust_id': self.broker_entrust_id,
                'name': self.name,
                'code': self.code,
                'type': self.type,
                'time': self.time.strftime('%Y-%m-%d %H:%M:%S') if self.time is not None else None,
                'price': self.price,
                'volume': self.volume,
                'volume_deal': self.volume_deal,
                'volume_cancel': self.volume_cancel,
                'status': self.status
                }
