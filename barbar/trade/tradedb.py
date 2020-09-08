from barbar.data.mongodb import MongoDB
from typing import Dict, List, Optional
import pandas as pd


class TradeDB(MongoDB):
    _trade_db = 'trade_db'  # 通用数据库
    _broker_info = 'broker_info'
    _user_info = 'user_info'
    _strategy_info = 'strategy_info'
    _account_info = 'account_info'  # 股票信息
    _entrust_info = 'entrust_info'
    _position_info = 'position_info'
    _signal_info = 'signal_info'
    _deal_info = 'deal_info'

    def __init__(self, uri='mongodb://localhost:27017/', pool=5):
        super().__init__(uri, pool)

    @property
    def user_info(self):
        return self.get_coll(self._trade_db, self._user_info)

    @property
    def strategy_info(self):
        return self.get_coll(self._trade_db, self._strategy_info)

    @property
    def account_info(self):
        return self.get_coll(self._trade_db, self._account_info)

    @property
    def entrust_info(self):
        return self.get_coll(self._trade_db, self._entrust_info)

    @property
    def position_info(self):
        return self.get_coll(self._trade_db, self._position_info)

    @property
    def deal_info(self):
        return self.get_coll(self._trade_db, self._deal_info)

    @property
    def signal_info(self):
        return self.get_coll(self._trade_db, self._signal_info)

    async def build_index(self):
        self.log.debug('创建索引...')

        self.log.debug('创建索引完成')

    def get_coll(self, db: str, col: str):
        client = self.get_client()
        if client is None:
            return None
        return client[db][col]

    async def load_user(self, **kwargs) -> Optional[Dict]:
        """
        :param kwargs:
        :return:
        """
        self.log.debug('查询用户, kwargs={} ...'.format(kwargs))
        data = await self.do_load(self.user_info, to_frame=False, **kwargs)
        self.log.debug('查询成功 data={}'.format(data))
        return data

    async def save_user(self, data: Dict) -> Optional[List[str]]:
        """
        :param
        :return: list[_id]
        """
        self.log.debug('保存用户信息, data = {} ...'.format(data))
        if data is not None:
            inserted_ids = await self.do_update(self.user_info, filter={'user_id': data['user_id']}, update=data)
            self.log.debug('保存用户信息成功, inserted_ids = {}'.format(inserted_ids))
            return inserted_ids
        return None

    async def load_account(self, **kwargs) -> Optional[Dict]:
        """
        :param kwargs:
        :return:
        """
        self.log.debug('查询账户, kwargs={} ...'.format(kwargs))
        data = await self.do_load(self.account_info, to_frame=False, **kwargs)
        self.log.debug('查询账户成功 size={}'.format(len(data) if data is not None else 0))
        return data
