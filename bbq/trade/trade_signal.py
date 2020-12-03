import uuid
from datetime import datetime


class TradeSignal:
    def __init__(self):
        self.signal_id = str(uuid.uuid4()).replace('-', '')

        self.source = ''  # 信号源, risk, strategy, broker, manual
        self.signal = ''  # sell, buy, cancel

        self.name = ''  # 股票名称
        self.code = ''  # 股票代码
        self.time = datetime.now()  # 时间

        self.price = 0.0
        self.volume = 0

        self.entrust_id = ''  # sell / cancel有效
