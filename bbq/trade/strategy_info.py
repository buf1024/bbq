from bbq.trade.account import Account
from bbq.trade.base_obj import BaseObj
from typing import Dict


class StrategyInfo(BaseObj):
    def __init__(self, account: Account):
        super().__init__(typ=account.typ, db_data=account.db_data, db_trade=account.db_trade, trader=account.trader)
        self.account_id = account.account_id
        self.strategy_id = ''
        self.strategy_opt = ''
        self.broker_id = ''
        self.broker_opt = ''
        self.risk_id = ''
        self.risk_opt = ''

        self.quot_opt = ''

    @BaseObj.discard_saver
    async def sync_to_db(self) -> bool:
        data = {'account_id': self.account_id, 'strategy_id': self.strategy_id,
                'strategy_opt': self.strategy_opt,
                'broker_id': self.broker_id, 'broker_opt': self.broker_opt,
                'risk_id': self.risk_id, "risk_opt": self.risk_opt,
                'quot_opt': self.quot_opt}
        await self.db_trade.save_strategy(data=data)
        return True

