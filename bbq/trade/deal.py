from bbq.trade.base_obj import BaseObj
import json


class Deal(BaseObj):
    def __init__(self, deal_id: str, entrust_id: str, account):
        super().__init__(typ=account.typ, db_data=account.db_data, db_trade=account.db_trade, trader=account.trader)
        self.account = account

        self.deal_id = deal_id
        self.entrust_id = entrust_id

        self.name = ''  # 股票名称
        self.code = ''  # 股票代码
        self.time = None  # 时间

        self.price = 0.0  # 价格
        self.volume = 0  # 量

        self.fee = 0

    @BaseObj.discard_saver
    async def sync_to_db(self) -> bool:
        data = {'account_id': self.account.account_id,
                'deal_id': self.deal_id, 'entrust_id': self.entrust_id,
                'name': self.name, 'code': self.code,
                'volume': self.volume, 'price': self.price,
                'fee': self.fee, 'time': self.time
                }
        await self.db_trade.save_deal(data=data)
        return True

    def to_dict(self):
        return {'account_id': self.account.account_id,
                'deal_id': self.deal_id, 'entrust_id': self.entrust_id,
                'name': self.name, 'code': self.code,
                'volume': self.volume, 'price': self.price,
                'fee': self.fee,
                'time': self.time.strftime('%Y-%m-%d %H:%M:%S') if self.time is not None else None
                }
