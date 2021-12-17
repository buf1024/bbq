from bbq.trade.account import Account
from .broker import Broker


class BrokerGitee(Broker):
    """
    委托应答Comment:
    status: cancel, deal, part_deal
    volume_cancel: cancel是必选
    volume_deal: deal/part_deal是必选

    事件Issue:
    事件pos_sync:
    name: ''  # 股票名称
    code: ''  # 股票代码
    time: 'yyyy-mm-dd HH:MM:SS'  # 首次建仓时间
    volume: 0  # 持仓量
    fee: 0.0  # 持仓费用
    price: 0.0  # 平均持仓价
    事件fund_sync:
    cash_init:
    cash_available:
    """

    def __init__(self, broker_id, account: Account):
        super().__init__(broker_id=broker_id, account=account)

        self.task_running = False
