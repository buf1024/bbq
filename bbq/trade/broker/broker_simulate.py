from .broker import Broker


class BrokerSimulate(Broker):

    def __init__(self, repo):
        super().__init__(repo)

    def init(self, **kwargs):
        super().init(**kwargs)

    def serialize(self):
        super().serialize()

    def on_strategy(self, payload):
        return super().on_strategy(payload)

    def on_quot(self, payload):
        super().on_quot(payload)
