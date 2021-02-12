from bbq.trade.base_obj import BaseObj
from datetime import datetime
from typing import Dict


class TradeSignal(BaseObj):
    sig_sell, sig_buy, sig_cancel = 'sell', 'buy', 'cancel'
    risk, strategy, broker, robot = 'risk', 'strategy', 'broker', 'robot'

    def __init__(self, signal_id: str, account, *,
                 source: str = '',  source_name: str = '', signal: str = '',
                 code: str = '', name: str = '', price: float = 0.0,  volume: int = 0,
                 time: datetime = None, entrust_id: str = ''):
        super().__init__(typ=account.typ, db_data=account.db_data, db_trade=account.db_trade, trader=account.trader)
        self.account = account

        self.signal_id = signal_id
        self.source = source  # 信号源, risk:xxx, strategy:xx, broker:xx, robot:
        self.source_name = source_name
        self.signal = signal  # sell, buy, cancel

        self.name = name  # 股票名称
        self.code = code  # 股票代码
        self.time = time  # 时间

        self.price = price
        self.volume = volume

        self.entrust_id = entrust_id  # sell / cancel 有效

    @BaseObj.discard_saver
    async def sync_to_db(self) -> bool:
        data = {'account_id': self.account.account_id,
                'signal_id': self.signal_id, 'name': self.name, 'code': self.code,
                'source': self.source, 'source_name': self.source_name, 'signal': self.signal,
                'volume': self.volume, 'price': self.price,
                'time': self.time
                }
        await self.db_trade.save_signal(data=data)
        return True

    def to_dict(self):
        return {'account_id': self.account.account_id,
                'signal_id': self.signal_id, 'name': self.name, 'code': self.code,
                'source': self.source, 'source_name': self.source_name, 'signal': self.signal,
                'volume': self.volume, 'price': self.price,
                'time': self.time.strftime('%Y-%m-%d %H:%M:%S') if self.time is not None else None
                }
