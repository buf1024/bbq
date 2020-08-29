from common import *
from threading import Thread
import log
import queue
from eventbus import emit, on

from quot import quotes
from selector.strategy import strategies as select_strategies
from trader.broker import brokers
from trader.strategy import strategies as trade_strategies
from trader.robot import robots


class Trader(Thread):

    def __init__(self, repo, mod, **kwargs):
        super().__init__()
        self.log = log.get_logger(self.__class__.__name__)
        self.repo = repo
        self.mod = mod
        self.kwargs = kwargs

        self.robot = None  # todo

        self.running = False

        self.strategies = []
        self._queue = queue.Queue()

        evt_bind(cat='trader', func=self.on_broker, target=self)
        evt_bind(cat='trader', func=self.on_quot, target=self)
        evt_bind(cat='trader', func=self.on_strategy, target=self)

    def set_robot(self, robot):
        self.robot = robot

    def add_strategy(self, strategy):
        if strategy is None:
            return False
        for s in self.strategies:
            if s.name == strategy.name and s.broker.name == strategy.broker.name:
                return False

        self.strategies.append(strategy)

        return True

    def remove_strategy(self, strategy):
        if strategy is None:
            return False
        for i, s in enumerate(self.strategies):
            if s.name == strategy.name and s.broker.name == strategy.broker.name:
                self.strategies.pop(i)
                return True
        return False

    def _get_codes(self):
        codes = []
        for strategy in self.strategies:
            codes = set(codes).union(set(strategy.codes))
        return codes

    def start(self):
        if self.robot is not None:
            self.robot.start()

        self.running = True
        super().start()

    def run(self):
        timeout = 0 if self.mod == 'backtest' else 1
        while self.running:
            try:
                self._queue.get(timeout=timeout)
            except Empty:
                codes = self._get_codes()
                print('request quot: {}'.format(codes))
                kwargs = {} if self.kwargs is None else self.kwargs
                quot = self.repo.quot.get_rt_quot(codes=codes, **kwargs)
                if quot is not None:
                    # 变化才发送，过滤掉停牌 开市期间才搞
                    disp = False
                    for c, q in quot.items():
                        if q is not None:
                            disp = True
                            break
                    if not disp:
                        self.running = False
                        break

                    for strategy in self.strategies:
                        emit(cat='trader', event='evt_on_quot', payload=(strategy.broker, quot))
                        emit(cat='trader', event='evt_on_quot', payload=(strategy, quot))
                else:
                    self.running = False
                    break

        print('quot done')
        bus_stop()

    @staticmethod
    def _on_event(payload, func_name):
        dest, payload = payload
        if dest is not None:
            func = getattr(dest, func_name)
            if func is not None:
                func(payload)

    @on(cat='trader', event='evt_on_quot', thread=True)
    def on_quot(self, payload):
        self._on_event(payload, 'on_quot')

    @on(cat='trader', event='evt_on_broker', thread=True)
    def on_broker(self, payload):
        self._on_event(payload, 'on_broker')

    @on(cat='trader', event='evt_on_strategy', thread=True)
    def on_strategy(self, payload):
        self._on_event(payload, 'on_strategy')


@singleton
class TraderRepository(BaseRepository):
    def __init__(self, config_path):
        super().__init__(config_path)


if __name__ == '__main__':
    def help(broker=None, strategy=None, robot=None):
        if broker is not None:
            print('broker:')
            print('name: {}\ndesc: {}\n'.format(broker.name, broker.desc))
        if strategy is not None:
            print('strategy:')
            print('name: {}\ndesc: {}\n'.format(strategy.name, strategy.desc))
        if robot is not None:
            print('robot:')
            print('name: {}\ndesc: {}\n'.format(robot.name, robot.desc))

        if broker is None and strategy is None and robot is None:
            print('brokers:')
            for cls in brokers.values():
                print('name: {}\ndesc: {}\n'.format(cls.name, cls.desc))
            print('strategies:')
            for cls in trade_strategies.values():
                print('name: {}\ndesc: {}\n'.format(cls.name, cls.desc))
            print('robots:')
            for cls in robots.values():
                print('name: {}\ndesc: {}\n'.format(cls.name, cls.desc))


    config_path, opts = parse_arguments(opt_desc='?=broker|strategy|robot=name '
                                                 'strategy=name;arg1=v1;arg2=v2 '
                                                 'broker=name;arg1=v1;arg2=v2 '
                                                 'robot=name;arg1=v1;arg2=v2 ')
    if config_path is None or opts is None:
        print('parse_arguments failed')
        os._exit(-1)

    if '?' in opts:
        topic = opts['?']
        if not isinstance(topic, dict):
            help()
            os._exit(-1)

        for k, v in topic.items():
            if k == 'broker':
                names = brokers.keys()
                if v not in names:
                    print('invalid broker name: {}, available names: {}'.format(v, names))
                else:
                    help(broker=brokers[v])

            if k == 'strategy':
                names = trade_strategies.keys()
                if v not in names:
                    print('invalid strategy name: {}, available names: {}'.format(v, names))
                else:
                    help(strategy=trade_strategies[v])

            if k == 'robot':
                names = robots.keys()
                if v not in names:
                    print('invalid robot name: {}, available names: {}'.format(v, names))
                else:
                    help(robot=robots[v])

        os._exit(0)

    strategy_name = opts['strategy']['strategy']
    broker_name = opts['broker']['broker']
    robot_name = opts['robot']['robot']

    strategy_cls = trade_strategies[strategy_name]
    broker_cls = brokers[broker_name]
    robot_cls = robots[robot_name]

    repo = TraderRepository(config_path)
    if not repo.init('trade'):
        print('req init failed')
        os._exit(-1)

    broker = broker_cls(repo, **opts['broker'])
    strategy = strategy_cls(repo, broker, **opts['strategy'])

    trader = Trader(repo, 'trade')
    trader.set_robot(robot_cls(repo, **opts['robot']))
    trader.add_strategy(strategy)

    trader.start()
    trader.join()
    bus_stop()
