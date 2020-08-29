"""
日志
setup_logger 设置logger可覆盖本日志逻辑,
logger_func 函数原型 def _get_logger(name, level, file)

"""
import logging
from logging.handlers import TimedRotatingFileHandler

_g_logs = {}

_g_file = None
_g_level = logging.DEBUG


def _get_logger(name, level, file):
    log = logging.getLogger(name)
    if name not in _g_logs:
        log.setLevel(level)
        ch = logging.StreamHandler()
        ch.setLevel(level)
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        ch.setFormatter(formatter)
        log.addHandler(ch)
        if file is not None:
            ch = TimedRotatingFileHandler(file, 'D', 1)
            ch.setLevel(level)
            ch.setFormatter(formatter)
            log.addHandler(ch)
    return log


def setup_logger(file=None, level="debug", logger_func=_get_logger):
    global _g_logger_func
    global _g_file
    global _g_level

    if logger_func is not None:
        _g_logger_func = logger_func

    level = logging.getLevelName(level.upper())
    _g_file = file
    _g_level = level


_g_logger_func = _get_logger


def get_logger(name='BARBAR'):
    return _g_logger_func(name, _g_level, _g_file)
