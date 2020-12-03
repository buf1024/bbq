from .strategy import Strategy
from ..account import Account


class Dummy(Strategy):
    def __init__(self, strategy_id, account: Account):
        super().__init__(strategy_id=strategy_id, account=account)
