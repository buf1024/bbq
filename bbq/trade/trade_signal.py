from bbq.trade.base_obj import BaseObj


class TradeSignal(BaseObj):
    def __init__(self, signal_id: str, account):
        super().__init__(typ=account.typ, db_data=account.db_data, db_trade=account.db_trade, trader=account.trader)
        self.account = account

        self.signal_id = signal_id
        self.source = ''  # 信号源, risk, strategy, broker, manual
        self.signal = ''  # sell, buy, cancel

        self.name = ''  # 股票名称
        self.code = ''  # 股票代码
        self.time = None  # 时间

        self.price = 0.0
        self.volume = 0

        self.entrust_id = ''  # sell / cancel 有效

    @BaseObj.discard_saver
    async def sync_to_db(self) -> bool:
        data = {'account_id': self.account.account_id,
                'signal_id': self.signal_id, 'name': self.name, 'code': self.code,
                'source': self.source, 'signal': self.signal,
                'volume': self.volume, 'price': self.price,
                'time': self.time
                }
        await self.db_trade.save_signal(data=data)
        return True

    def to_dict(self):
        return {'account_id': self.account.account_id,
                'signal_id': self.signal_id, 'name': self.name, 'code': self.code,
                'source': self.source, 'signal': self.signal,
                'volume': self.volume, 'price': self.price,
                'time': self.time.strftime('%Y-%m-%d %H:%M:%S') if self.time is not None else None
                }
