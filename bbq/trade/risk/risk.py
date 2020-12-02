import json
from bbq.trade.base_obj import BaseObj
from bbq.trade.account import Account
from typing import Dict, Optional


class Risk(BaseObj):
    def __init__(self, risk_id, account: Account):
        super().__init__(typ=account.typ, db_data=account.db_data, db_trade=account.db_trade)
        self.account = account
        self.risk_id = risk_id

        self.opt = None

    async def init(self, opt: Optional[Dict]):
        return True

    async def destroy(self):
        pass

    def on_broker(self, payload):
        raise Exception('{} not implement'.format(self.on_broker.__qualname__))

    def on_quot(self, payload):
        raise Exception('{} not implement'.format(self.on_quot.__qualname__))

    async def sync_from_db(self) -> bool:
        opt = await self.db_trade.load_strategy(filter={'account_id': self.account.account_id},
                                                projection=['risk_opt'], limit=1)
        opt = None if len(opt) == 0 else opt[0]
        if opt is not None and 'risk_opt' in opt:
            try:
                opt['risk_opt'] = json.loads(opt['risk_opt'])
            except Exception as e:
                self.log.error('risk from db to json error: {}, str={}'.format(e, opt['risk_opt']))
                return False
        return await self.init(opt=opt['risk_opt'])

    @BaseObj.discard_saver
    async def sync_to_db(self) -> bool:
        data = {'account_id': self.account.account_id,
                'risk_id': self.risk_id,
                'risk_opt': json.dumps(self.opt) if self.opt is not None else None}
        await self.db_trade.save_strategy(data=data)
        return True
