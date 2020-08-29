from functools import wraps
import os
import importlib
import asyncio


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

            module = importlib.import_module('{}.{}'.format(package + package_suf, file_name[:-3]))

            file_names = file_name[:-3].split('_')
            name_list = [file_name.capitalize() for file_name in file_names]
            cls_name = ''.join(name_list)
            cls = module.__getattribute__(cls_name)
            if cls is not None:
                if len(package_suf) > 0:
                    package_suf = package_suf[1:] + '.'
                strategy[package_suf + cls_name] = cls
            else:
                print(
                    'warning: file {} not following strategy naming convention'.format(root_path + os.sep + file_name))

    return strategy


def run_until_complete(*coro):
    loop = None
    try:
        loop = asyncio.get_event_loop()
        loop.run_until_complete(
            asyncio.gather(*coro)
        )
    finally:
        if loop is not None:
            loop.close()


if __name__ == '__main__':
    @singleton
    class B:
        def __init__(self, js=None):
            print('class a')


    print('cls b1={}, b2={}'.format(id(B()), id(B())))

    import barbar.fetch as fetch
    fetch.tdx.join()
    print(fetch.get_code_list())
    print(fetch.fetcher)

