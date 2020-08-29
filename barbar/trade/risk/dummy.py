from trader.risk.risk import Risk


class Dummy(Risk):
    def __init__(self, repo, account, **kwargs):
        super().__init__(repo, account, **kwargs)

    def name(self):
        return 'dummy'

    def desc(self):
        return 'dummy risk strategy'

    def on_broker(self, payload):
        print('risk on_broker: {}'.format(payload))

    def on_quot(self, payload):
        print('risk on_quot: {}'.format(payload))
