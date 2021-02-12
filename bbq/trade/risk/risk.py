import json
from bbq.trade.action_obj import BaseActionObj
from bbq.trade.account import Account
from typing import Dict, Optional


class Risk(BaseActionObj):
    """
    风控类接口，可产生信号:
    1. evt_sig_cancel 取消委托
    2. evt_sig_buy 委托买
    3. evt_sig_sell 委托卖
    """

    def __init__(self, risk_id, account: Account):
        super().__init__(typ=account.typ, db_data=account.db_data, db_trade=account.db_trade, trader=account.trader)
        self.account = account
        self.risk_id = risk_id

        self.opt = None

    def name(self):
        return self.__class__.__name__

    async def init(self, opt: Optional[Dict]):
        return True

    async def destroy(self):
        return True

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

    async def on_quot(self, evt, payload):
        pass

    async def sync_from_db(self) -> bool:
        opt = await self.db_trade.load_strategy(filter={'account_id': self.account.account_id},
                                                projection=['risk_opt'], limit=1)
        opt = None if len(opt) == 0 else opt[0]
        risk_opt = None
        if opt is not None and 'risk_opt' in opt:
            risk_opt = opt['risk_opt']
        return await self.init(opt=risk_opt)

    @BaseActionObj.discard_saver
    async def sync_to_db(self) -> bool:
        data = {'account_id': self.account.account_id,
                'risk_id': self.risk_id,
                'risk_opt': json.dumps(self.opt) if self.opt is not None else None}
        await self.db_trade.save_strategy(data=data)
        return True
