import log


class Strategy:
    def __init__(self, repo):
        self.log = log.get_logger(self.__class__.__name__)
        self.rep = repo
        self.db = None
        self.quot = None

    def name(self):
        """
        策略名称
        :return:
        """
        raise Exception('{} not implement'.format(self.name.__qualname__))

    def usage(self):
        """
        策略描述，使用方式
        :return:
        """
        raise Exception('{} not implement'.format(self.usage.__qualname__))

    def argument(self):
        return None

    def init(self, db=None, quot=None, **kwargs):
        """
        初始化接口
        :param db:
        :param quot:
        :param kwargs:
        :return: True/False
        """
        self.db = db
        self.quot = quot

        return True

    def stop(self):
        """
        停止接口
        :return: True/False
        """
        return True

    def select(self, **kwargs):
        """
        根据策略，选择股票
        :param kwargs:
        :return: [code, code, ...]/None
        """
        raise Exception('{} not implement'.format(self.select.__qualname__))

    def regression(self, codes=None, **kwargs):
        """
        根据策略，对股票进行回归
        :param codes:
        :param kwargs:
        :return: 策略匹配度 0 ~ 1
        """
        raise Exception('{} not implement'.format(self.regression.__qualname__))

