from barbar.data.mongodb_sync import MongoDBSync
from typing import List, Optional
import pandas as pd


class StockDB(MongoDBSync):
    _comm_db = 'comm_db'  # 通用数据库
    _code_info = 'code_info'  # 股票信息
    _index_info = 'index_info'  # 指数信息
    _trade_cal = 'trade_cal'  # 交日日历

    _bar_db = 'bar_db'  # K线数据库
    _stock_bar = 'stock_bar'  # 股票日k
    _index_bar = 'index_bar'  # 大盘日k
    _adj_factor = 'adj_factor'  # 复权因子

    _misc_db = 'misc_db'  # 其他杂项数据库
    _block_info = 'block_info'  # 板块信息
    _xdxr_info = 'xdxr_info'  # 除权除息

    def __init__(self, uri='mongodb://localhost:27017/', pool=5):
        super().__init__(uri, pool)

    @property
    def code_info(self):
        return self.get_coll(self._comm_db, self._code_info)

    @property
    def index_info(self):
        return self.get_coll(self._comm_db, self._index_info)

    @property
    def trade_cal(self):
        return self.get_coll(self._comm_db, self._trade_cal)

    def stock_bar(self, code: str):
        return self.get_coll(self._bar_db, self._stock_bar + '_' + code)

    def index_bar(self, code: str):
        return self.get_coll(self._bar_db, self._index_bar + '_' + code)

    def adj_factor(self, code: str):
        return self.get_coll(self._bar_db, self._adj_factor + '_' + code)

    @property
    def block_info(self):
        return self.get_coll(self._misc_db, self._block_info)

    @property
    def xdxr_info(self):
        return self.get_coll(self._misc_db, self._xdxr_info)

    def build_index(self):
        self.log.debug('创建索引...')
        self.code_info.create_index([('code', 1)], unique=True)
        self.index_info.create_index([('code', 1)], unique=True)
        self.trade_cal.create_index([('cal_date', -1)], unique=True)
        datas = self.load_code_list(projection=['code'])
        if datas is not None:
            for data in datas.to_dict('records'):
                self.stock_bar(data['code']).create_index([('code', 1), ('trade_date', -1)], unique=True)
                self.adj_factor(data['code']).create_index([('code', 1), ('trade_date', -1)], unique=True)
        datas = await self.load_code_list(projection=['code'])
        if datas is not None:
            for data in datas.to_dict('records'):
                self.index_bar(data['code']).create_index([('code', 1), ('trade_date', -1)], unique=True)
        self.block_info.create_index([('blockname', 1)], unique=False)
        self.xdxr_info.create_index([('code', 1)], unique=False)
        self.log.debug('创建索引完成')

    def get_coll(self, db: str, col: str):
        client = self.get_client()
        if client is None:
            return None
        return client[db][col]

    def load_code_list(self, **kwargs) -> Optional[pd.DataFrame]:
        """
        kwargs参数同pymongo参数, 另外增加to_frame
        :param kwargs:  filter=None, projection=None, skip=0, limit=0, sort=None, to_frame=True
        :return: DataFrame[_id, code name]
        """
        self.log.debug('加载股票列表, kwargs={} ...'.format(kwargs))
        df = self.do_load(self.code_info, **kwargs)
        self.log.debug('加载股票列表成功 size={}'.format(df.shape[0] if df is not None else 0))
        return df

    def save_code_list(self, data: pd.DataFrame) -> List[str]:
        """
        :param data: DataFrame[code name]
        :return: list[_id]
        """
        self.log.debug('保存股票列表, count = {} ...'.format(data.shape[0] if data is not None else 0))
        inserted_ids = self.do_batch_update(data, lambda x: (self.code_info, {'code': x['code']}, x))
        self.log.debug('保存股票列表成功, size = {}'.format(len(inserted_ids)))
        return inserted_ids

    def load_trade_cal(self, **kwargs) -> Optional[pd.DataFrame]:
        """
        :param kwargs:  filter=None, projection=None, skip=0, limit=0, sort=None, to_frame=True
        :return: DataFrame[_id, cal_date,is_open]
        """
        self.log.debug('加载交易日历, kwargs={} ...'.format(kwargs))
        df = self.do_load(self.trade_cal, **kwargs)
        self.log.debug('加载交易日历成功 size={}'.format(df.shape[0] if df is not None else 0))
        return df

    def save_trade_cal(self, data: pd.DataFrame) -> List[str]:
        """
        :param data: DataFrame[cal_date,is_open]
        :return: list[_id]
        """
        self.log.debug('保存交易日历, count = {} ...'.format(data.shape[0] if data is not None else 0))
        inserted_ids = self.do_batch_update(data, lambda x: (self.trade_cal, {'cal_date': x['cal_date']}, x))
        self.log.debug('保存交易日历成功, size = {}'.format(len(inserted_ids)))
        return inserted_ids

    def load_adj_stock_bar(self, code: str = None, fq: str = None, **kwargs) -> Optional[pd.DataFrame]:
        """

        :param code:
        :param fq: qfq 前复权 hfq 后复权 None不复权
        :param kwargs:  filter=None, projection=None, skip=0, limit=0, sort=None, to_frame=True
        :return: DataFrame([code,trade_date,open,high,low,close,vol,amount])
        """
        self.log.debug('加载复权因子日线, code={}, kwargs={}'.format(code, kwargs))

        srt = None if 'sort' not in kwargs else kwargs['sort']
        if srt is None:
            srt = [('trade_date', -1)]
            kwargs['sort'] = srt
        else:
            srt = [s for s in srt if srt != ('trade_date', -1)]
            srt.append(('trade_date', -1))

        data = self.load_stock_bar(code=code, **kwargs)
        if data is None or data.empty:
            self.log.error('加载复权日线失败, 无日线数据')
            return None
        if fq == 'qfq' or fq == 'hfq':
            factors = self.load_adj_factor(code=code, sort=[('trade_date', -1)])
            if factors is None or factors.empty:
                self.log.error('加载复权因子日线失败, 无复权数据')
                return None

            data = data.merge(factors, how='left', left_on=['code', 'trade_date'], right_on=['code', 'trade_date'])

            factor = 1.0
            if fq == 'qfq':
                # 前复权以现价为标准
                factor = factors['adj_factor'].iloc[0]
                data.fillna(method='ffill', inplace=True)
            if fq == 'hfq':
                # 前复权以第一天上市价为标准
                factor = factors['adj_factor'].iloc[-1]
                data.fillna(method='bfill', inplace=True)

            data['open'] = data['open'] * data['adj_factor'] / factor
            data['high'] = data['high'] * data['adj_factor'] / factor
            data['low'] = data['low'] * data['adj_factor'] / factor
            data['close'] = data['close'] * data['adj_factor'] / factor
            data['vol'] = data['vol'] * data['adj_factor'] / factor

            data.drop(columns=['adj_factor'], inplace=True)

        self.log.debug('加载日线数据成功 size={}'.format(data.shape[0] if data is not None else 0))

        return data

    def load_stock_bar(self, code: str, **kwargs) -> Optional[pd.DataFrame]:
        """

        :param code:
        :param kwargs:  filter=None, projection=None, skip=0, limit=0, sort=None, to_frame=True
        :return: DataFrame([code,trade_date,open,high,low,close,vol,amount])
        """
        self.log.debug('加载日K数据, code={}, kwargs={} ...'.format(code, kwargs))
        df = self.do_load(self.stock_bar(code=code), **kwargs)
        self.log.debug('加载日K数据成功 size={}'.format(df.shape[0] if df is not None else 0))
        return df

    def save_stock_bar(self, code: str, data: pd.DataFrame) -> List[str]:
        """

        :param code:
        :param data: DataFrame([code,trade_date,open,high,low,close,vol,amt,adj_factor])
        :return: None/list[_id]
        """
        self.log.debug('保存日K数据, code={}, count = {} ...'.format(code, data.shape[0] if data is not None else 0))
        inserted_ids = []
        if data is not None:
            docs = data.to_dict('records')
            result = self.stock_bar(code=code).insert_many(docs)
            inserted_ids = result.inserted_ids
        self.log.debug('保存日K数据成功, size = {}'.format(len(inserted_ids)))
        return inserted_ids

    def load_adj_factor(self, code: str, **kwargs) -> Optional[pd.DataFrame]:
        """

        :param code:
        :param kwargs:  filter=None, projection=None, skip=0, limit=0, sort=None, to_frame=True
        :return: DataFrame([code,trade_date,adj_factor])
        """
        self.log.debug('加载复权因子数据, code={}, kwargs={} ...'.format(code, kwargs))
        df = self.do_load(self.adj_factor(code=code), **kwargs)
        self.log.debug('加载复权因子数据成功 size={}'.format(df.shape[0] if df is not None else 0))
        return df

    def save_adj_factor(self, code: str, data: pd.DataFrame) -> List[str]:
        """

        :param code:
        :param data: DataFrame([code,trade_date,adj_factor])
        :return: None/list[_id]
        """
        self.log.debug('保存复权因子数据, code={}, count = {} ...'.format(code, data.shape[0] if data is not None else 0))
        inserted_ids = []
        if data is not None:
            docs = data.to_dict('records')
            result = self.adj_factor(code=code).insert_many(docs)
            inserted_ids = result.inserted_ids
        self.log.debug('保存复权因子数据成功, size = {}'.format(len(inserted_ids)))
        return inserted_ids

    def load_index_list(self, **kwargs) -> Optional[pd.DataFrame]:
        """
        指数基本信息
        :return: None/DataFrame([code,name,market,category,index_type,exp_date])
        """
        self.log.debug('加载大盘指数列表, kwargs={} ...'.format(kwargs))
        df = self.do_load(self.index_info, **kwargs)
        self.log.debug('加载大盘指数列表成功 size={}'.format(df.shape[0] if df is not None else 0))
        return df

    def save_index_list(self, data: pd.DataFrame) -> List[str]:
        """
        :param data: DataFrame([code,name,market,category,index_type,exp_date])
        :return: None/list[_id]
        """
        self.log.debug('保存大盘指数列表, count = {} ...'.format(data.shape[0]))
        inserted_ids = self.do_batch_update(data, lambda x: (self.index_info, {'code': x['code']}, x))
        self.log.debug('保存大盘指数列表成功, size = {}'.format(len(inserted_ids)))
        return inserted_ids

    def load_index_bar(self, code: str, **kwargs) -> Optional[pd.DataFrame]:
        """

        :param code:
        :param kwargs:
        :return: None/DataFrame([code,trade_date,open,high,low,close,vol,amount])
        """
        self.log.debug('加载大盘日k数据, code{}, kwargs={} ...'.format(code, kwargs))
        df = self.do_load(self.index_bar(code=code), **kwargs)
        self.log.debug('加载大盘日k数据成功 size={}'.format(df.shape[0] if df is not None else 0))
        return df

    def save_index_bar(self, code: str, data: pd.DataFrame) -> List[str]:
        """
        :param code:
        :param data:  DataFrame([code,trade_date,open,high,low,close,vol,amt])
        :return: None/list[_id]
        """
        self.log.debug('保存大盘日K数据, code={} count = {} ...'.format(code, data.shape[0]))
        inserted_ids = []
        if data is not None:
            docs = data.to_dict('records')
            result = self.index_bar(code=code).insert_many(docs)
            inserted_ids = result.inserted_ids
        self.log.debug('保存大盘日K数据成功, size = {}'.format(len(inserted_ids)))
        return inserted_ids

    def load_block_list(self, **kwargs) -> Optional[pd.DataFrame]:
        """
        kwargs参数同pymongo参数, 另外增加to_frame
        :param kwargs:  filter=None, projection=None, skip=0, limit=0, sort=None, to_frame=True
        :return: DataFrame[_id, code name]
        """
        self.log.debug('加载板块列表, kwargs={} ...'.format(kwargs))
        df = self.do_load(self.block_info, **kwargs)
        self.log.debug('加载板块列表成功 size={}'.format(df.shape[0] if df is not None else 0))
        return df

    def save_block_list(self, data: pd.DataFrame) -> List[str]:
        """
        :param data: DataFrame[code name]
        :return: list[_id]
        """
        self.log.debug('保存板块列表, count = {} ...'.format(data.shape[0] if data is not None else 0))
        inserted_ids = self.do_batch_update(data, lambda x: (self.block_info, {'code': x['code']}, x))
        self.log.debug('保存板块列表成功, size = {}'.format(len(inserted_ids)))
        return inserted_ids

    def load_xdxr_list(self, **kwargs) -> Optional[pd.DataFrame]:
        """

        :param kwargs:  filter=None, projection=None, skip=0, limit=0, sort=None, to_frame=True
        :return: DataFrame([blockname, code])
        """
        self.log.debug('加载除权除息数据, kwargs={} ...'.format(kwargs))
        df = self.do_load(self.xdxr_info, **kwargs)
        self.log.debug('加载除权除息数据成功 size={}'.format(df.shape[0] if df is not None else 0))
        return df

    def save_xdxr_list(self, data: pd.DataFrame) -> List[str]:
        """

        :param data: DataFrame([blockname, code])
        :return: None/list[_id]
        """
        self.log.debug('保存除权除息数据, count = {} ...'.format(data.shape[0] if data is not None else 0))
        inserted_ids = self.do_batch_update(data,
                                            lambda x: (self.xdxr_info, {'code': x['code'], 'date': x['date'],
                                                                        'category': x['category']}, x))
        self.log.debug('保存除权除息数据成功, size = {}'.format(len(inserted_ids)))
        return inserted_ids


if __name__ == '__main__':
    import barbar.fetch as fetch
    from datetime import datetime
    from barbar.config import conf_dict
    import sys

    fetch.init()

    def test_trade_cal(db):
        cals = fetch.get_trade_cal()
        print('fetch cal:\n')
        print(cals.head())
        db.save_trade_cal(cals)
        end_date = datetime(year=2020, month=8, day=5)
        start_date = datetime(year=2010, month=8, day=4)
        cals = db.load_trade_cal(filter={'cal_date': {'$lte': end_date, '$gte': start_date}, 'is_open': 1},
                                 sort=[('cal_date', -1)])
        print('cals:\n')
        print(cals.head())


    def test_code_list(db):
        codes = fetch.get_code_list()
        print('fetch codes:\n')
        print(codes.head())
        db.save_code_list(codes)
        codes = db.load_code_list()
        print('codes:\n')
        print(codes.head())


    def test_index_list(db):
        indexes = fetch.get_index_list()
        print('fetch index:\n')
        print(indexes.head())
        db.save_index_list(indexes)
        indexes = db.load_index_list()
        print('indexes:\n')
        print(indexes.head())


    def test_stock_bar(db):
        kdata = fetch.get_bar(code='000001.sz')
        print('fetch kdata:\n')
        print(kdata.head())
        db.save_stock_bar(kdata)
        kdata = db.load_stock_bar(filter={'code': '000001.SZ'})
        print('mongo kdata:\n')
        print(kdata.head())


    def test_adj_factor(db):
        factor = fetch.get_adj_factor(code='000001.sz')
        print('fetch kdata:\n')
        print(factor.head())
        db.save_adj_factor(factor)
        factor = db.load_adj_factor(filter={'code': '000001.SZ'})
        print('mongo kdata:\n')
        print(factor.head())


    def test_adj_stock_bar(db):
        kdata = db.load_adj_stock_bar(code='000001.sz')
        print('mongo kdata fq=None:\n')
        print(kdata.head())
        kdata = db.load_adj_stock_bar(code='000001.sz', fq='qfq',
                                      filter={'trade_date': {'$gte': datetime(year=2020, month=8, day=17),
                                                             '$lte': datetime(year=2020, month=8, day=25)}})
        print('mongo kdata fq=qfq:\n')
        print(kdata.head())
        kdata = db.load_adj_stock_bar(code='000001.sz', fq='hfq',
                                      filter={'trade_date': {'$gte': datetime(year=2020, month=8, day=17),
                                                             '$lte': datetime(year=2020, month=8, day=25)}})
        print('mongo kdata fq=hfq:\n')
        print(kdata.head())


    def test_index_bar(db):
        kdata = fetch.get_index_bar(code='000001.SH')
        print('fetch index kdata:\n')
        print(kdata.head())
        db.save_index_bar(kdata)
        kdata = db.load_index_bar(filter={'code': '000001.SH'})
        print('mongo index kdata:\n')
        print(kdata.head())


    def test_block_list(db):
        block = fetch.get_block_list()
        print('fetch block:\n')
        print(block.head())
        db.save_block_list(block)
        block = db.load_block_list()
        print('block:\n')
        print(block.head())


    def test_xdxr_list(db):
        xdxr = fetch.get_xdxr_list(code='000001.sz')
        print('fetch xdxr:\n')
        print(xdxr.head())
        db.save_xdxr_list(xdxr)
        xdxr = db.load_xdxr_list()
        print('xdxr:\n')
        print(xdxr.head())


    def test_build_index(db):
        db.build_index()


    mongo = StockDB(uri=conf_dict['mongo']['uri'], pool=conf_dict['mongo']['pool'])
    if not mongo.init():
        print('init stockdb failed.')
        sys.exit(-1)

    #
    # test_trade_cal(mongo)
    # test_code_list(mongo)
    # test_index_list(mongo)
    # test_stock_bar(mongo)
    # test_adj_factor(mongo)
    # test_adj_stock_bar(mongo)
    # test_index_bar(mongo)
    # test_block_list(mongo)
    # test_xdxr_list(mongo)
    test_build_index(mongo)
