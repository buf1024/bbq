import json
from bbq.trade.base_obj import BaseObj
from bbq.trade.account import Account
from typing import Optional, Dict


class Broker(BaseObj):
    """
    券商交易接口

    提供 buy(买), sell(卖), cancel(撤销) 委托接口
    buy(买), sell(卖), cancel(撤销)委托成功或失败均产生委托结果事件
    buy(买), sell(卖), cancel(撤销)成交或撤销均产生事件

    产生的事件:
    1. 委托(买,卖,撤销)提交事件
    2. 委托(买,卖,撤销)成交事件
    3. 资金同步事件
    4. 持仓同步事件
    """

    def __init__(self, broker_id, account: Account):
        super().__init__(typ=account.typ, db_data=account.db_data, db_trade=account.db_trade)
        self.account = account
        self.broker_id = broker_id

        self.opt = None

    async def init(self, opt: Optional[Dict]):
        return True

    async def destroy(self):
        pass

    def on_strategy(self, payload):
        raise Exception('{} on_strategy payload={}, not implement'.format(self.__class__.__name__, payload))

    def on_quot(self, payload):
        # if self.account is not None:
        #     self.account.on_quot(payload=payload)
        pass

    async def sync_from_db(self) -> bool:
        opt = await self.db_trade.load_strategy(filter={'account_id': self.account.account_id},
                                                projection=['broker_opt'], limit=1)
        opt = None if len(opt) == 0 else opt[0]
        broker_opt = None
        if opt is not None and 'broker_opt' in opt:
            broker_opt = opt['broker_opt']
        return await self.init(opt=broker_opt)

    @BaseObj.discard_saver
    async def sync_to_db(self) -> bool:
        data = {'account_id': self.account.account_id,
                'broker_id': self.broker_id,
                'broker_opt': json.dumps(self.opt) if self.opt is not None else None}
        await self.db_trade.save_strategy(data=data)
        return True
