import uuid


class Entrust:
    def __init__(self):
        self.id = uuid.uuid4()
        self.broker_id = None
        self.type = None  # buy, sell, cancel
        self.status = 'commit'  # commit deal cancel
        self.name = None
        self.code = None

        self.time = None

        self.price = None
        self.volume = None
        self.volume_deal = 0
        self.volume_cancel = 0
