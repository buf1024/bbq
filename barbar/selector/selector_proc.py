import threading
import log
from common import *
import selector
import multiprocessing


class Selector:
    def __init__(self, repo, **kwargs):
        super().__init__()
        self.log = log.get_logger(self.__class__.__name__)
        self.repo = repo
        self.db = repo.db
        self.kwargs = kwargs

        self.cmd_dict = {
            'list_strategies': self._do_list,
            'strategy': self._do_select
        }

    def _do_list(self):
        out_msg = ''
        for name, cls in selector.strategy.items():
            out_msg += '{}:\n{}\n\n'.format(name, cls().usage())
        return out_msg

    def _do_select(self):
        strategy = self.kwargs['strategy']
        strategy_args = {} if 'strategy_args' not in opts else self.kwargs['strategy_args']

        if strategy not in selector.strategy:
            out_msg = '策略{}不在策略库中，请检查策略名称'.format(strategy)
            self.log.error(out_msg)
            return None

        self.log.info('初始化策略{}实例'.format(strategy))
        inst = selector.strategy[strategy]()
        if not inst.init(self.repo.db, self.repo.quot, **strategy_args):
            out_msg = '初始策略{}失败，请检查日志'.format(strategy)
            self.log.error(out_msg)
            return None

        self.log.info('执行策略{}选股'.format(strategy))
        codes = inst.select(**strategy_args)
        self.log.info('策略{}, 选择{}'.format(strategy, codes))

        return codes

    def run_cmd(self):
        # 同时存在 list_strategies 和 strategy 优先执行 list_strategies
        if 'list_strategies' in self.kwargs:
            return self.cmd_dict['list_strategies']()
        if 'strategy' in self.kwargs:
            return self.cmd_dict['strategy']()

        return self.cmd_dict['list_strategies']()


@singleton
class SelectorRepository(BaseRepository):
    def __init__(self, config_path):
        super().__init__(config_path)


def selector_proc():
    config_path, opts = parse_arguments(
        opt_desc='list_strategies strategy=[strategy name] strategy_args=args '
                 'e.g.: strategy=Random strategy_args=count=10;market=SZ')
    if config_path is None or opts is None:
        print('parse_arguments failed')
        os._exit(-1)

    repo = SelectorRepository(config_path)
    if not repo.init('selector'):
        print('req init failed')
        os._exit(-1)

    s = Selector(repo, **opts)
    print(s.run_cmd())
    bus_stop()


if __name__ == '__main__':
    selector_proc()
