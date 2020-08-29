import log
import json
from datetime import datetime
from trader.risk import strategy as risk_strategies
from selector.strategy import strategy as select_strategies

'''
{
    "strategy": {
        "name": 'test',
        "risk": {
            "name": ""
        },
        "select": {
            "name": ""
        }
    },
    "broker": {
        "name": "ths",
        "account": {
        }
    },
    "status": "running",
    "create_time": "",
    "update_time": ""
}
'''

'''

'''

class Strategy:
    name = None
    desc = None

    def __init__(self, repo, broker=None, **kwargs):
        self.log = log.get_logger(self.__class__.__name__)
        self.repo = repo
        self.db = repo.db
        self.broker = broker

        self.account = None if broker is None else broker.account
        self.db = repo.db

        self.risk = None
        self.select = None
        self.codes = []

        self._create_time = datetime.now()

    def init(self, **kwargs):
        if self.name is not None and \
                self.broker is not None and \
                self.broker.name is not None:
            if not self._load_strategy():
                return False

        return True

    def start(self):
        return True

    def stop(self):
        self._stop()
        self._save_strategy('running')

    def destroy(self):
        self._stop()
        self._save_strategy('stop')

    def serialize(self):
        pass

    def on_open(self, period):
        pass

    def on_close(self, period):
        pass

    def on_quot(self, payload):
        pass

    def on_broker(self, payload):
        pass

    def _load_strategy(self):
        data = self.db.load_strategy_info(
            filter={'strategy.name': self.name, 'broker.name': self.broker.name, 'status': 'running'},
            limit=1, to_frame=False)
        if data is not None:
            self._create_time = data['create_time']
            if 'risk' in data['strategy']:
                risk_data = data['strategy']['risk']
                if self.risk is None:
                    self.risk = risk_strategies[risk_data['name']](self.repo)
                if not self.risk.init(data=risk_data):
                    self.log.error('strategy risk {} init failed'.format(risk_data['name']))
                    return False

            if 'select' in data['strategy']:
                select_data = data['strategy']['select']
                if self.select is None:
                    self.select = select_strategies[select_data['name']](self.repo)
                if not self.select.init(data=select_data):
                    self.log.error('strategy select {} init failed'.format(select_data['name']))
                    return False

            if not self.broker.init(data=data['broker']):
                self.log.error('broker init failed')
                return False

    def _save_strategy(self, status):
        data = {
            'strategy': self.serialize(),
            'broker': self.broker.serialize(),
            'status': status,
            'create_time': self._create_time,
            'update_time': datetime.now()
        }
        self.db.save_strategy_info(
            filter={'strategy.name': self.name, 'broker.name': self.name}, data=data)

    def _stop(self):
        self.risk.stop()
        self.select.stop()
        self.broker.stop()
