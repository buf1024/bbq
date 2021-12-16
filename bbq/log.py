"""
日志
"""
import logging
from logging.handlers import TimedRotatingFileHandler
from typing import Any


class _ColorFormatter(logging.Formatter):
    blue = '\x1b[34;21m'
    green = '\x1b[32;21m'
    yellow = '\x1b[33;21m'
    red = '\x1b[31;21m'
    bold_red = '\x1b[31;1m'
    reset = '\x1b[0m'

    def __init__(self, fmt=None, datefmt='%Y-%m-%d %H:%M:%S', color=False):
        super().__init__(fmt=fmt, datefmt=datefmt)
        self.color = color
        self.fmt = {
            logging.DEBUG: self.blue + fmt + self.reset,
            logging.INFO: self.green + fmt + self.reset,
            logging.WARNING: self.yellow + fmt + self.reset,
            logging.ERROR: self.red + fmt + self.reset,
            logging.CRITICAL: self.bold_red + fmt + self.reset
        }

    def format(self, record: logging.LogRecord) -> str:
        if self.color:
            fmt = self.fmt.get(record.levelno)
            formatter = logging.Formatter(fmt)
            return formatter.format(record)

        return super().format(record)


class _LogContext:
    def __init__(self, file=None, level=logging.DEBUG, color=False):
        self.is_inited = False

        self.color = color
        self.file = file
        self.level = level
        self.loggers = {}
        self.tmp_loggers = {}
        self.tmp_handlers = {}
        self.file_handler = None
        self.stream_handler = None
        self.formatter = '[%(asctime)s][%(levelname).1s][%(name)s]  %(message)s'

    def setup_logger(self, file=None, level=logging.DEBUG, color=False):
        self.is_inited = True

        self.color = color
        self.file = file
        self.level = level

        for name, my_log in self.tmp_loggers.items():
            my_log.log.removeHandler(self.tmp_handlers[name]['handler'])
            self.get_logger(name=name, prefix=self.tmp_handlers[name]['prefix'])
        self.tmp_loggers.clear()
        self.tmp_handlers.clear()

    def get_file_handler(self):
        if self.file_handler is not None:
            return self.file_handler

        if self.file is not None:
            self.file_handler = TimedRotatingFileHandler(self.file, 'D', 1, encoding='utf-8')
            self.file_handler.setFormatter(logging.Formatter(fmt=self.formatter, datefmt='%Y-%m-%d %H:%M:%S'))
            return self.file_handler
        return None

    def get_stream_handler(self):
        if self.stream_handler is not None:
            return self.stream_handler
        self.stream_handler = logging.StreamHandler()
        self.stream_handler.setFormatter(_ColorFormatter(fmt=self.formatter, datefmt='%Y-%m-%d %H:%M:%S', color=self.color))
        return self.stream_handler

    def get_logger(self, name, prefix):
        if self.is_inited:
            if name in self.loggers:
                return self.loggers[name]

            log = logging.getLogger(name)
            log.setLevel(self.level)
            ch = self.get_stream_handler()
            ch.setLevel(self.level)
            log.addHandler(ch)
            if self.file is not None:
                ch = self.get_file_handler()
                ch.setLevel(self.level)
                log.addHandler(ch)
            self.loggers[name] = _MyLogger(log=log, prefix=prefix)
            return self.loggers[name]

        if name in self.tmp_loggers:
            return self.tmp_loggers[name]

        log = logging.getLogger(name)
        log.setLevel(self.level)
        ch = logging.StreamHandler()
        ch.setFormatter(_ColorFormatter(fmt=self.formatter, datefmt='%Y-%m-%d %H:%M:%S', color=self.color))
        ch.setLevel(self.level)
        log.addHandler(ch)
        self.tmp_loggers[name] = _MyLogger(log=log, prefix=prefix)
        self.tmp_handlers[name] = dict(handler=ch, prefix=prefix)
        return self.tmp_loggers[name]


class _MyLogger:
    def __init__(self, log: logging.Logger, prefix=None):
        self.log = log
        self.prefix = prefix

    def get_msg(self, msg):
        if self.prefix is not None:
            return self.prefix + ' ' + msg
        return msg

    def debug(self, msg: Any, *args: Any, **kwargs: Any) -> None:
        return self.log.debug(self.get_msg(msg), *args, **kwargs)

    def info(self, msg: Any, *args: Any, **kwargs: Any) -> None:
        return self.log.info(self.get_msg(msg), *args, **kwargs)

    def warn(self, msg: Any, *args: Any, **kwargs: Any) -> None:
        return self.log.warning(self.get_msg(msg), *args, **kwargs)

    def warning(self, msg: Any, *args: Any, **kwargs: Any) -> None:
        return self.log.warning(self.get_msg(msg), *args, **kwargs)

    def error(self, msg: Any, *args: Any, **kwargs: Any) -> None:
        return self.log.error(self.get_msg(msg), *args, **kwargs)

    def critical(self, msg: Any, *args: Any, **kwargs: Any) -> None:
        return self.log.critical(self.get_msg(msg), *args, **kwargs)


_g_ctx = _LogContext()


def setup_logger(file=None, level='debug', color=False, reset=False):
    global _g_ctx
    if reset:
        _g_ctx = _LogContext()
    _g_ctx.setup_logger(file=file, level=logging.getLevelName(level.upper()), color=color)


def get_logger(name='BBQ', prefix=None):
    global _g_ctx
    return _g_ctx.get_logger(name=name, prefix=prefix)

# if __name__ == '__main__':
#     setup_logger('./abc.log')
#     log = get_logger(name='test', prefix='meme')
#
#     log.debug('debug log')
#     log.info('info log')
#     log.warning('info log')
#     log.error('info log')
#     log.critical('info log')
#
#     log = get_logger(name='test2', prefix=None)
#     log.info('info log')
#     log.debug('debug log')
