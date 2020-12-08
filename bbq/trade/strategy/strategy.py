import json
from bbq.trade.base_obj import BaseObj
from bbq.trade.account import Account
from typing import Dict, Optional

'''
{
    "strategy": {
        "name": 'test',
        "risk": {
            "name": ""
        },
        "select": {
            "name": ""
        }
    },
    "broker": {
        "name": "ths",
        "account": {
        }
    },
    "status": "running",
    "create_time": "",
    "update_time": ""
}
'''

'''

'''


class Strategy(BaseObj):
    def __init__(self, strategy_id, account: Account):
        super().__init__(typ=account.typ, db_data=account.db_data, db_trade=account.db_trade, trader=account.trader)
        self.account = account
        self.strategy_id = strategy_id

        self.opt = None

    async def init(self, opt: Optional[Dict]):
        return True

    async def destroy(self):
        pass

    async def on_open(self, period):
        pass

    async def on_close(self, period):
        pass

    async def on_quot(self, evt, payload):
        self.log.info('strategy on_quot: evt={}, payload={}'.format(evt, payload))

    async def on_broker(self, payload):
        pass

    async def sync_from_db(self) -> bool:
        opt = await self.db_trade.load_strategy(filter={'account_id': self.account.account_id},
                                                projection=['strategy_opt'], limit=1)
        opt = None if len(opt) == 0 else opt[0]
        strategy_opt = None
        if opt is not None and 'strategy_opt' in opt:
            strategy_opt = opt['strategy_opt']
        return await self.init(opt=strategy_opt)

    @BaseObj.discard_saver
    async def sync_to_db(self) -> bool:
        data = {'account_id': self.account.account_id,
                'strategy_id': self.strategy_id,
                'strategy_opt': json.dumps(self.opt) if self.opt is not None else None}
        await self.db_trade.save_strategy(data=data)
        return True

