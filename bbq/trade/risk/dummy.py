from .risk import Risk
from ..account import Account


class Dummy(Risk):
    """
    不作任何风控
    """
    def __init__(self, risk_id, account: Account):
        super().__init__(risk_id=risk_id, account=account)
