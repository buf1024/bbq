import uuid
from datetime import datetime


class Signal:
    def __init__(self):
        self.signal_id = str(uuid.uuid4())

        self.source = ''  # 信号源, risk, strategy, broker, handy
        self.action = ''  # sell, buy, cancel

        self.signaled_id = ''  # 关联id

        self.name = ''  # 股票名称
        self.code = ''  # 股票代码
        self.time = datetime.now()  # 时间

        self.price = 0.0
        self.volume = 0


