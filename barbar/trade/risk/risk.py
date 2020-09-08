import barbar.log as log


class Risk:
    def __init__(self, repo, **kwargs):
        self.log = log.get_logger(self.__class__.__name__)
        self.repo = repo

    def init(self, **kwargs):
        """
        初始化接口
        :param db:
        :param quot:
        :param kwargs:
        :return: True/False
        """

        return True

    def stop(self):
        """
        停止接口
        :return: True/False
        """
        return True

    def on_broker(self, payload):
        """
        根据策略，选择股票
        :param kwargs:
        :return: [code, code, ...]/None
        """
        raise Exception('{} not implement'.format(self.on_broker.__qualname__))

    def on_quot(self, payload):
        """
        根据策略，对股票进行回归
        :param codes:
        :param kwargs:
        :return: 策略匹配度 0 ~ 1
        """
        raise Exception('{} not implement'.format(self.on_quot.__qualname__))
