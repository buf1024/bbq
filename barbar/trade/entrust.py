import uuid
from datetime import datetime


class Entrust:
    def __init__(self):
        self.entrust_id = str(uuid.uuid4())

        self.name = ''  # 股票名称
        self.code = ''  # 股票代码
        self.time = datetime.now()  # 委托时间

        self.broker_entrust_id = ''  # broker对应的委托id
        self.broker_id = ''  # 对应的broker
        self.type = None  # buy, sell, cancel
        self.status = 'commit'  # commit 已提交 deal 已成 part_deal 部成 cancel 已经取消

        self.price = 0.0  # 价格
        self.volume = 0  # 量
        self.volume_deal = 0  # 已成量
        self.volume_cancel = 0  # 已取消量
