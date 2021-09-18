import bbq.log as log
import pandas as pd
from typing import Optional


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

    async def select(self) -> Optional[pd.DataFrame]:
        """
        根据策略，选择股票
        :return: [{code, ctx...}, {code, ctx}, ...]/None
        """
        raise Exception('选股策略 {} 没实现选股函数'.format(self.__class__.__name__))

    async def run(self, count=10, **kwargs) -> Optional[pd.DataFrame]:
        if not await self.init(**kwargs):
            self.log.error('策略 {} 初始化失败'.format(self.__class__.__name__))
            return None

        data = await self.select()
        if data is not None and not data.empty:
            if len(data) > count:
                data = data[:count]
        await self.destroy()

        return data
