from bbq.data.mongodb import MongoDB
import pandas as pd


class FundDB(MongoDB):
    _fund_db = 'fund_db'  # 通用数据库
    _fund_info_col = 'fund_info'  # 基金信息
    _fund_block_col = 'fund_block'  # 基金板块信息
    _fund_net_db = 'fund_net_db'  # 通用数据库
    _fund_net_col = 'fund_net'  # 基金净值信息

    def __init__(self, uri='mongodb://localhost:27017/', pool=5):
        super().__init__(uri, pool)

    @property
    def fund_info(self):
        return self.get_coll(self._fund_db, self._fund_info_col)

    @property
    def fund_block(self):
        return self.get_coll(self._fund_db, self._fund_block_col)

    def fund_net(self, code):
        return self.get_coll(self._fund_net_db, self._fund_net_col + '_' + code)

    def get_coll(self, db: str, col: str):
        client = self.get_client()
        if client is None:
            return None
        return client[db][col]

    async def build_index(self):
        self.log.debug('创建基金索引...')
        await self.fund_info.create_index(index=[('code', 1)], unique=True)
        await self.fund_block.create_index(index=[('plate', -1), ('name', -1)], unique=False)
        datas = await self.load_fund_info(projection=['code'])
        if datas is not None:
            for data in datas.to_dict('records'):
                await self.fund_net(data['code']).create_index(index=[('date', -1)], unique=True)
        self.log.debug('创建基金索引完成')

    async def load_block_list(self, **kwargs):
        self.log.debug('加载基金板块列表, kwargs={}'.format(kwargs))
        df = await self.do_load(self.fund_block, **kwargs)
        self.log.debug('加载基金板块列表成功, size={}'.format(df.shape[0] if df is not None else 0))
        return df

    async def save_block_list(self, data: pd.DataFrame):
        self.log.debug('保存基金板块列表, count = {}'.format(data.shape[0]))
        inserted_ids = await self.do_batch_update(data=data,
                                                  func=lambda x: (
                                                      self.fund_block, {'plate': x['plate'], 'name': x['name']}, x))
        self.log.debug('保存基金板块列表成功, size = {}'.format(len(inserted_ids) if inserted_ids is not None else 0))
        return inserted_ids

    async def load_fund_info(self, **kwargs):
        self.log.debug('加载基金列表, kwargs={}'.format(kwargs))
        df = await self.do_load(self.fund_info, **kwargs)
        self.log.debug('加载基金列表成功, size={}'.format(df.shape[0] if df is not None else 0))
        return df

    async def save_fund_info(self, data):
        self.log.debug('保存基金列表, count = {}'.format(data.shape[0]))
        inserted_ids = await self.do_batch_update(data=data,
                                                  func=lambda x: (self.fund_info, {'code': x['code']}, x))
        self.log.debug('保存基金列表成功, size = {}'.format(len(inserted_ids) if inserted_ids is not None else 0))
        return inserted_ids

    async def load_fund_net(self, code: str, **kwargs):
        self.log.debug('加载基金净值数据, kwargs={}'.format(kwargs))
        df = await self.do_load(self.fund_net(code=code), **kwargs)
        self.log.debug('加载基金净值数据,成功, size={}'.format(df.shape[0] if df is not None else 0))
        return df

    async def save_fund_net(self, code: str, data: pd.DataFrame):
        self.log.debug('保存基金净值数据, count = {} ...'.format(data.shape[0]))
        inserted_ids = await self.do_batch_update(data=data,
                                                  func=lambda x: (self.fund_net(code=code), {'date': x['date']}, x))
        self.log.debug('保存基金净值成功, size = {}'.format(len(inserted_ids) if inserted_ids is not None else 0))
        return inserted_ids


if __name__ == '__main__':
    from bbq.fetch.fund_eastmoney import FundEastmoney
    from bbq.common import run_until_complete
    from bbq.config import conf_dict
    import sys


    async def test_fund_info(db, east):
        df = await east.get_fund_info(code='160220')
        if df is not None:
            print('test_fund_info web:\n', df)
            await db.save_fund_info(df)

            df = await db.load_fund_info(filter={'code': '160220'})
            print('test_fund_info db:\n', df)

    async def test_fund_block_info(db, east):
        df = await east.get_block_list()
        if df is not None:
            print('test_block_info web:\n', df.head())
            await db.save_block_list(df)

            df = await db.load_block_list()
            print('test_block_info db:\n', df.head())


    async def test_fund_net(db, east):
        df = await east.get_fund_net(code='000021')
        if df is not None:
            print('test_fund_net web:\n')
            print(df.head())
            await db.save_fund_net(df)

            df = await db.load_fund_net(filter={'code': '000021'})
            print('test_fund_net db:\n')
            print(df.head())


    async def test_build_index(db, east):
        await db.build_index()
        print('done')

    mongo = FundDB(uri=conf_dict['mongo']['uri'], pool=conf_dict['mongo']['pool'])
    if not mongo.init():
        print('init fundDB failed.')
        sys.exit(-1)

    fund = FundEastmoney()

    run_until_complete(
        # test_fund_info(mongo, fund),
        # test_fund_block_info(mongo, fund),
        test_fund_net(mongo, fund),
        # test_build_index(mongo, fund)
    )
