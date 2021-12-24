import asyncio
import base64
import importlib
import os
from functools import wraps
import yaml
import bbq.log as log
import sys
from IPython.display import display


def singleton(cls):
    insts = {}

    @wraps(cls)
    def wrapper(*args, **kwargs):
        if cls.__qualname__ not in insts:
            insts[cls.__qualname__] = cls(*args, **kwargs)
            cls.inst = insts[cls.__qualname__]
        return insts[cls.__qualname__]

    return wrapper


def load_strategy(dir_path, package, exclude=()):
    """
    strategy文件只能有一个类，类名为文件名(首字母大写), 如文件明带_, 去掉后，后面单词首字母大写
    :param dir_path: 文件所在目录
    :param package: 文件所在包名
    :param exclude: 排除的文件， 默认__开头的文件都会排除
    :return:
    """
    if len(dir_path) > 0 and dir_path[0] == '~':
        dir_path = os.path.expanduser('~') + dir_path[1:]

    strategy = {}
    for root_path, dirs, files in os.walk(dir_path):
        if root_path.find('__') >= 0 or root_path.startswith('.'):
            continue

        package_suf = ''
        if dir_path != root_path:
            package_suf = '.' + root_path[len(dir_path) + 1:].replace(os.sep, '.')

        for file_name in files:
            if not file_name.endswith('.py'):
                continue

            if file_name.startswith('__') or file_name.startswith('.') or file_name in exclude:
                continue
            module_str = '{}.{}'.format(package + package_suf, file_name[:-3])
            if module_str.startswith('.'):
                module_str = module_str[1:]
            module = importlib.import_module(module_str)

            file_names = file_name[:-3].split('_')
            name_list = [file_name.capitalize() for file_name in file_names]
            cls_name = ''.join(name_list)
            cls = module.__getattribute__(cls_name)
            if cls is not None:
                suffix = package_suf
                if len(suffix) > 0:
                    suffix = suffix[1:] + '.'
                strategy[suffix + cls_name] = cls
            else:
                print(
                    'warning: file {} not following strategy naming convention'.format(root_path + os.sep + file_name))

    return strategy


def run_until_complete(*coro):
    loop = None
    try:
        loop = asyncio.get_event_loop()
        return loop.run_until_complete(asyncio.gather(*coro))
    finally:
        if loop is not None:
            loop.close()


def setup_log(conf_dict, file_name, reset=False):
    log.setup_logger(file=conf_dict['log']['path'] + os.sep + file_name,
                     level=conf_dict['log']['level'],
                     reset=reset)
    logger = log.get_logger()
    logger.debug('初始化数日志')

    return logger


def setup_db(conf_dict, cls):
    db = cls(uri=conf_dict['mongo']['uri'], pool=conf_dict['mongo']['pool'])
    if not db.init():
        raise Exception('初始化数据库失败')

    return db


def load_cmd_yml(value):
    js = None
    for i in range(2):
        try:
            if i != 0:
                value = base64.b64decode(str.encode(value, encoding='utf-8'))
            js = yaml.load(value, yaml.FullLoader)
        except Exception as e:
            if i != 0:
                print('e={}, not legal yml string/base64 encode yml string, please check'.format(e))
    return js


def prepare_strategy(data_dict, data_builtin, paths):
    builtin_dict = data_dict['builtin']
    if len(builtin_dict) == 0:
        strategy_data = load_strategy(*data_builtin)
        if len(strategy_data) > 0:
            builtin_dict.update(strategy_data)

    external_dict = data_dict['external']
    if paths is not None:
        for path in paths:
            if path not in sys.path:
                sys.path.append(path)
            strategy_data = load_strategy(path, '')
            if len(strategy_data) > 0:
                external_dict.update(strategy_data)


def get_strategy(data_dict, name):
    name_pair = name.split(':')
    if len(name_pair) != 2:
        return None

    d = data_dict['builtin'] if name_pair[0] == 'builtin' else data_dict['external']

    if name_pair[1] not in d:
        return None

    return d[name_pair[1]]


def is_alive(pid):
    try:
        os.kill(pid, 0)
    except OSError:
        return False
    else:
        return True


def pretty_df(df):
    display(df.style.set_properties(**{
        'text-align': 'left',
        'white-space': 'pre-wrap'
    }))
