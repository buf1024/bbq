import os
import os.path
import json
from typing import Tuple


def __init_config__() -> Tuple:
    __home = os.path.expanduser('~')

    barbar_path = __home + os.sep + '.config' + os.sep + 'bbq'
    log_path = barbar_path + os.sep + 'logs'
    conf_file = barbar_path + os.sep + 'config.json'

    os.makedirs(barbar_path, exist_ok=True)
    os.makedirs(log_path, exist_ok=True)

    conf_dict = dict(
        log=dict(level='debug', path=log_path),
        mongo=dict(uri='mongodb://localhost:27017/', pool=5),
        nats=dict(uri='nats://127.0.0.1:4222'),
        tushare_pro=dict(token='408481e156da6a5facd695e58add4d0bf705649fe0f460d03d4d6908')
    )

    if not os.path.exists(conf_file):
        conf_str = json.dumps(conf_dict, ensure_ascii=False, indent=2)
        with open(conf_file, 'w') as f:
            f.write(conf_str)
    else:
        with open(conf_file) as f:
            conf_dict = json.load(f)

    log_dict = conf_dict['log']
    log_dict['level'] = os.getenv('LOG_LEVEL', log_dict['level'])
    log_dict['path'] = os.getenv('LOG_PATH', log_dict['path'])

    mongo_dict = conf_dict['mongo']
    mongo_dict['uri'] = os.getenv('MONGO_URI', mongo_dict['uri'])
    mongo_dict['pool'] = int(os.getenv('MONGO_POOL', mongo_dict['pool']))

    nats_dict = conf_dict['nats']
    nats_dict['uri'] = os.getenv('NATS_URI', nats_dict['uri'])

    tushare_dict = conf_dict['tushare_pro']
    tushare_dict['token'] = os.getenv('TUSHARE_TOKEN', tushare_dict['token'])

    return barbar_path, conf_file, conf_dict


barbar_path, conf_file, conf_dict = __init_config__()
