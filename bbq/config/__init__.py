import os
import os.path
from typing import Tuple
import yaml


def init_def_config() -> Tuple:
    home = os.path.expanduser('~')

    bbq_path = home + os.sep + '.config' + os.sep + 'bbq'
    log_path = bbq_path + os.sep + 'logs'
    conf_file = bbq_path + os.sep + 'config.yml'

    strategy_base_path = bbq_path + os.sep + 'strategy'
    strategy_select_path = strategy_base_path + os.sep + 'strategy' + os.sep + 'select'
    strategy_trade_path = strategy_base_path + os.sep + 'strategy' + os.sep + 'trade'
    strategy_risk_path = strategy_base_path + os.sep + 'strategy' + os.sep + 'risk'

    os.makedirs(bbq_path, exist_ok=True)
    os.makedirs(log_path, exist_ok=True)
    os.makedirs(strategy_select_path, exist_ok=True)
    os.makedirs(strategy_trade_path, exist_ok=True)
    os.makedirs(strategy_risk_path, exist_ok=True)

    conf_dict = dict(
        log=dict(level='debug', path=log_path),
        mongo=dict(uri='mongodb://localhost:27017/', pool=10),
        strategy=dict(select=[strategy_select_path],
                      trade=[strategy_trade_path],
                      risk=[strategy_risk_path])
    )

    if not os.path.exists(conf_file):
        conf_str = yaml.dump(conf_dict)
        with open(conf_file, 'w') as f:
            f.write(conf_str)
    else:
        with open(conf_file) as f:
            conf_dict = yaml.load(f.read(), yaml.FullLoader)

    log_dict = conf_dict['log']
    log_dict['level'] = os.getenv('LOG_LEVEL', log_dict['level'])
    log_dict['path'] = os.getenv('LOG_PATH', log_dict['path'])

    mongo_dict = conf_dict['mongo']
    mongo_dict['uri'] = os.getenv('MONGO_URI', mongo_dict['uri'])
    mongo_dict['pool'] = int(os.getenv('MONGO_POOL', mongo_dict['pool']))

    return conf_file, conf_dict


def init_config(conf_file: str) -> Tuple:
    if not os.path.exists(conf_file):
        home = os.path.expanduser('~')
        config_file = home + os.sep + conf_file

    if not os.path.exists(conf_file):
        return conf_file, None

    conf_dict = None
    with open(conf_file) as f:
        conf_dict = yaml.load(f.read(), yaml.FullLoader)

    return conf_file, conf_dict
