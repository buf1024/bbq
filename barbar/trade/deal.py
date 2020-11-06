import uuid
from datetime import datetime


class Deal:
    def __init__(self):
        self.deal_id = str(uuid.uuid4()).replace('-', '')
        self.entrust_id = str(uuid.uuid4()).replace('-', '')

        self.name = ''  # 股票名称
        self.code = ''  # 股票代码
        self.time = datetime.now()  # 时间

        self.price = 0.0  # 价格
        self.volume = 0  # 量

