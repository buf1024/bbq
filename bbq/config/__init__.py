import os
import os.path
import json
from typing import Tuple


def __init_config__() -> Tuple:
    __home = os.path.expanduser('~')

    __bbq_path = __home + os.sep + '.config' + os.sep + 'bbq'
    log_path = __bbq_path + os.sep + 'logs'
    __conf_file = __bbq_path + os.sep + 'config.json'

    os.makedirs(__bbq_path, exist_ok=True)
    os.makedirs(log_path, exist_ok=True)

    __conf_dict = dict(
        log=dict(level='debug', path=log_path),
        mongo=dict(uri='mongodb://localhost:27017/', pool=5),
    )

    if not os.path.exists(__conf_file):
        conf_str = json.dumps(__conf_dict, ensure_ascii=False, indent=2)
        with open(__conf_file, 'w') as f:
            f.write(conf_str)
    else:
        with open(__conf_file) as f:
            __conf_dict = json.load(f)

    log_dict = __conf_dict['log']
    log_dict['level'] = os.getenv('LOG_LEVEL', log_dict['level'])
    log_dict['path'] = os.getenv('LOG_PATH', log_dict['path'])

    mongo_dict = __conf_dict['mongo']
    mongo_dict['uri'] = os.getenv('MONGO_URI', mongo_dict['uri'])
    mongo_dict['pool'] = int(os.getenv('MONGO_POOL', mongo_dict['pool']))

    return __bbq_path, __conf_file, __conf_dict


bbq_path, conf_file, conf_dict = __init_config__()
