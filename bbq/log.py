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

_file_handler = None
_stream_handler = None
_formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')


def _get_file_handler(file):
    global _file_handler
    if _file_handler is not None:
        return _file_handler
    if file is not None:
        _file_handler = TimedRotatingFileHandler(file, 'D', 1, encoding='utf-8')
        _file_handler.setFormatter(_formatter)
        return _file_handler
    return None


def _get_stream_handler():
    global _stream_handler
    if _stream_handler is not None:
        return _stream_handler
    _stream_handler = logging.StreamHandler()
    _stream_handler.setFormatter(_formatter)
    return _stream_handler


def _get_logger(name, level, file):
    log = logging.getLogger(name)
    if name not in _g_logs:
        log.setLevel(level)
        ch = _get_stream_handler()
        ch.setLevel(level)
        log.addHandler(ch)
        if file is not None:
            ch = _get_file_handler(file)
            ch.setLevel(level)
            log.addHandler(ch)
        _g_logs[name] = log
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

    for name, log in _g_logs.items():
        log.setLevel(level=level)
        if file is not None:
            log.removeHandler(_file_handler)
            ch = _get_file_handler(file)
            ch.setLevel(level)
            log.addHandler(ch)


_g_logger_func = _get_logger


def get_logger(name='BBQ'):
    return _g_logger_func(name, _g_level, _g_file)
