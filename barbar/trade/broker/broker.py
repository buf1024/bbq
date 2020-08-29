import log
from trader.account import Account


class Broker:
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

    name = None
    desc = None

    def __init__(self, repo):
        self.log = log.get_logger(self.__class__.__name__)
        self.repo = repo
        self.account = Account(repo)
        self.strategy = None

    def init(self, **kwargs):
        pass

    def serialize(self):
        pass

    def on_strategy(self, payload):
        raise Exception('{} on_strategy payload={}, not implement'.format(self.__class__.__name__, payload))

    def on_quot(self, payload):
        if self.account is not None:
            self.account.on_quot(payload=payload)
