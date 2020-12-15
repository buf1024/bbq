import json
from bbq.trade.base_obj import BaseObj
from bbq.trade.account import Account
from typing import Optional, Dict


class Broker(BaseObj):
    """
    券商类接口
    接收事件
    1. evt_broker_buy 买委托事件
    2. evt_broker_sell 卖委托事件
    3. evt_broker_cancel 撤销委托事件

    buy(买), sell(卖), cancel(撤销)委托成功或失败均产生委托结果事件
    buy(买), sell(卖), cancel(撤销)成交或撤销均产生事件

    券商产生的事件:
    1. evt_broker_commit 委托提交事件
    2. evt_broker_deal 委托(买,卖)成交事件
    3. evt_broker_cancel 撤销事件
    4. evt_broker_fund_sync 资金同步事件
    5. evt_broker_pos_sync 持仓同步事件
    """

    def __init__(self, broker_id, account: Account):
        super().__init__(typ=account.typ, db_data=account.db_data, db_trade=account.db_trade, trader=account.trader)
        self.account = account
        self.broker_id = broker_id

        self.opt = None

    async def init(self, opt: Optional[Dict]):
        return True

    async def destroy(self):
        pass

    async def on_open(self, evt, payload):
        """
        开始事件回调
        :param evt: evt_start/evt_morning_start/evt_noon_start
                    程序开始/交易日早市开始/交易日午市开始
        :param payload:
        :return:
        """
        pass

    async def on_close(self, evt, payload):
        """
        结束事件回调
        :param evt: evt_end/evt_morning_end/evt_noon_end
                    程序结束/交易日早市结束/交易日午市结束
        :param payload:
        :return:
        """
        pass

    async def on_entrust(self, evt, payload):
        """

        :param evt: evt_broker_buy/evt_broker_sell/evt_broker_cancel
        :param payload: Entrust
        :return:
        """
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
