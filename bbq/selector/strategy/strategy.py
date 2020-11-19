import bbq.log as log
from typing import List


class Strategy:
    def __init__(self, db):
        self.log = log.get_logger(self.__class__.__name__)
        self.db = db

    def desc(self):
        pass

    async def init(self, **kwargs):
        """
        初始化接口
        :param kwargs:
        :return: True/False
        """
        return True

    async def destroy(self):
        """
        清理接口
        :return: True/False
        """
        return True

    async def select(self):
        """
        根据策略，选择股票
        :param kwargs:
        :return: [code, code, ...]/None
        """
        raise Exception('{} not implement'.format(self.select.__qualname__))

    async def regression(self, codes: List[str]):
        """
        根据策略，对股票进行回归
        :param codes:
        :return: 策略匹配度 0 ~ 1
        """
        return 1.0
