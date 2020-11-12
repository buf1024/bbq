from .strategy import Strategy


class Dummy(Strategy):
    """
    选择股票：前3个交易日 连续上涨
    高于昨日最高价: 卖出
    低于昨日最低价: 买入
    """
    name = '随机测试策略'
    desc = '随机测试策略'

    def __init__(self, repo, **kwargs):
        super().__init__(repo, **kwargs)

    def init(self, **kwargs):
        if not super().init(**kwargs):
            return False



    def on_open(self, period):
        super().on_open(period)

    def on_quot(self, payload):
        print('trade on_quot: {}'.format(payload))

    def on_broker(self, payload):
        print('trade on_broker: {}'.format(payload))
