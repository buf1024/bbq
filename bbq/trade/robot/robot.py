from bbq.trade.base_obj import BaseObj
from ..account import Account
from typing import Optional, Dict


class Robot(BaseObj):
    """
    运维类接口，可产生运维事件和交易信号:
    1. evt_sig_cancel 取消委托
    2. evt_sig_buy 委托买
    3. evt_sig_sell 委托卖
    """
    def __init__(self, robot_id, account: Account):
        super().__init__(typ=account.typ, db_data=account.db_data, db_trade=account.db_trade, trader=account.trader)
        self.account = account
        self.robot_id = robot_id

        self.opt = None

    async def init(self, opt: Optional[Dict]) -> bool:
        return True

    async def destroy(self) -> bool:
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

    async def on_robot(self, evt, payload):
        self.log.info('robot on_robot: {}'.format(evt))
