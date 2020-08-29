from .risk import Risk
from datetime import datetime, timedelta


class SimpleStop(Risk):
    def __init__(self, repo, **kwargs):
        super().__init__(repo, **kwargs)

        self._stop_profit = 0
        self._stop_profit_rate = 0

        self._stop_lost = 0
        self._stop_lost_rate = 0

        self._stop_time = 0

    def name(self):
        return 'simple-stop'

    def desc(self):
        return 'simple stop'

    def init(self, **kwargs):
        self._stop_profit = 0 if 'stop_profit' not in kwargs else kwargs['stop_profit']
        self._stop_lost_rate = 0 if 'stop_lost_rate' not in kwargs else kwargs['stop_lost_rate']

        self._stop_lost = 0 if 'stop_lost' not in kwargs else kwargs['stop_lost']
        self._stop_lost_rate = 0 if 'stop_lost_rate' not in kwargs else kwargs['stop_lost_rate']

        self._stop_time = 0 if 'stop_time' not in kwargs else kwargs['stop_time']

    def on_broker(self, payload):
        print('risk on_broker: {}'.format(payload))

    def on_quot(self, payload):
        for postion in self.account.position.values():
            if postion.volume_available <= 0:
                continue

            is_lost = False if postion.profit > 0 else True

            profit_rate = abs(postion.profit_rate)
            profit = abs(postion.profit)
            if self._stop_lost_rate > 0 and is_lost and self._stop_lost_rate > profit_rate:
                continue

            if self._stop_lost > 0 and is_lost and self._stop_lost > profit:
                continue

            if self._stop_profit_rate > 0 and not is_lost and self._stop_profit_rate > profit_rate:
                continue

            if self._stop_profit and not is_lost and self._stop_lost > profit:
                continue

            if self._stop_time > 0:
                pass
