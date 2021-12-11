from bbq.data.mongodb import MongoDB
from typing import List, Optional
import pandas as pd


class StockDB(MongoDB):
    _meta = {
        # 股票信息
        'stock_info': {'code': '代码', 'name': '名称', 'listing_date': '上市日期', 'block': '板块', 'is_margin': '是否融资融券标的'},
        # 股票日线数据
        'stock_daily': {'code': '代码', 'trade_date': '交易日', 'close': '收盘价', 'open': '开盘价', 'high': '最高价', 'low': '最低价',
                        'volume': '成交量(股)', 'turnover': '换手率', 'hfq_factor': '后复权因子'},
        # 股票指标
        'stock_index': {'code': '代码', 'trade_date': '交易日', 'pe': '市盈率', 'pe_ttm': '市盈率TTM',
                        'pb': '市净率', 'ps': '市销率', 'ps_ttm': '市销率TTM', 'dv_ratio': '股息率', 'dv_ttm': '股息率TTM',
                        'total_mv': '总市值'},
        # 股票复权因子
        'stock_fq_factor': {'code': '代码', 'trade_date': '交易日', 'hfq_factor': '后复权因子', 'qfq_factor': '前复权因子',
                            'sync_date': '最近同步时间(避免全量同步程序使用)'},

        # 指数信息
        'index_info': {'code': '代码', 'name': '名称'},
        # 指数日线数据, 如: index_daily
        'index_daily': {'code': '代码', 'trade_date': '交易日', 'close': '收盘价', 'open': '开盘价', 'high': '最高价', 'low': '最低价',
                        'volume': '成交量(股)'},

        # 股票北向资金
        'stock_ns_flow': {'trade_date': '交易日',
                          'sz_north_value': '深股通北上', 'sh_north_value': '沪股通北上', 'north_value': '北上资金',
                          'sz_south_value': '深股通南下', 'sh_south_value': '沪股通南下', 'south_value': '南下资金'},

        # 历史分红数据
        'stock_his_divend': {'code': '代码', 'name': '名称', 'listing_date': '上市日期', 'divend_acc': '累计股息',
                             'divend_avg': '年均股息', 'divend_count': '分红次数', 'financed_total': '融资总额',
                             'financed_count': '融资次数', 'sync_date': '最近同步时间(避免全量同步程序使用)'},

        # 融资融券明细数据
        'stock_margin': {'code': '代码', 'name': '名称', 'trade_date': '交易日',
                         'spj': '收盘价(元)', 'zdf': '涨跌幅(%)',
                         'rzye': '融资余额(元)(RZYE)', 'rzyezb': '融资余额占流通市值比(%)(RZYEZB)', 'rzmre': '融资买入额(元)',
                         'rzche': '融资偿还额(元)', 'rzjme': '融资净买入(元)',
                         'rqye': '融券余额(元)', 'rqyl': '融券余量(股)', 'rqmcl': '融券卖出量(股)', 'rqchl': '融券偿还量(股)',
                         'rqjmg': '净卖出(股)',
                         'rzrqye': '融资融券余额(元)', 'rzrqyecz': '融资融券余额差值(元)'},

        # 申万行业数据
        'sw_index_info': {'index_code': '行业代码', 'index_name': '行业名称', 'stock_code': '股票代码', 'stock_name': '股票名称',
                          'start_date': '开始日期', 'weight': '权重'}
    }

    _db = 'bbq_stock_db'  # 股票数据库

    def __init__(self, uri='mongodb://localhost:27017/', pool=5):
        super().__init__(uri, pool)

    @property
    def stock_info(self):
        return self.get_coll(self._db, 'stock_info')

    @property
    def index_info(self):
        return self.get_coll(self._db, 'index_info')

    @property
    def stock_daily(self):
        return self.get_coll(self._db, 'stock_daily')

    @property
    def stock_index(self):
        return self.get_coll(self._db, 'stock_index')

    @property
    def stock_fq_factor(self):
        return self.get_coll(self._db, 'stock_fq_factor')

    @property
    def index_daily(self):
        return self.get_coll(self._db, 'index_daily')

    @property
    def stock_ns_flow(self):
        return self.get_coll(self._db, 'stock_ns_flow')

    @property
    def stock_his_divend(self):
        return self.get_coll(self._db, 'stock_his_divend')

    @property
    def stock_margin(self):
        return self.get_coll(self._db, 'stock_margin')

    @property
    def sw_index_info(self):
        return self.get_coll(self._db, 'sw_index_info')

    def test_coll(self):
        return self.stock_info

    async def load_stock_info(self, **kwargs) -> Optional[pd.DataFrame]:
        """
        kwargs参数同pymongo参数, 另外增加to_frame
        :param kwargs:  filter=None, projection=None, skip=0, limit=0, sort=None, to_frame=True
        :return: DataFrame
        """
        self.log.debug('加载股票信息, kwargs={} ...'.format(kwargs))
        df = await self.do_load(self.stock_info, **kwargs)
        self.log.debug('加载股票信息成功 size={}'.format(df.shape[0] if df is not None else 0))
        return df

    async def save_stock_info(self, data: pd.DataFrame) -> List[str]:
        """
        :param data: DataFrame[code name]
        :return: list[_id]
        """
        count = data.shape[0] if data is not None else 0
        inserted_ids = []
        self.log.debug('保存股票信息, count = {} ...'.format(count))
        if count > 0:
            inserted_ids = await self.do_insert(coll=self.stock_info, data=data)
        self.log.debug('保存股票信息成功, size = {}'.format(len(inserted_ids) if inserted_ids is not None else 0))
        return inserted_ids

    async def load_stock_daily(self, fq: str = None, **kwargs) -> Optional[pd.DataFrame]:
        """
        :param fq: qfq 前复权 hfq 后复权 None不复权
        :param kwargs:  filter=None, projection=None, skip=0, limit=0, sort=None, to_frame=True
        :return: DataFrame([code,trade_date,open,high,low,close,vol,amount])
        """
        self.log.debug('加载股票日线, kwargs={}'.format(kwargs))

        proj_tmp = kwargs['projection'] if 'projection' in kwargs else None
        proj = self._meta['stock_daily'].keys()
        kwargs['projection'] = proj

        df = await self.do_load(self.stock_daily, **kwargs)
        if df is None or df.shape[0] == 0:
            self.log.debug('加载日线数据成功 size=0')
            return None

        # 需按trade_date升序
        if fq == 'qfq' or fq == 'hfq':
            if fq == 'qfq':
                # df['open'] = df['open'] * df['qfq_factor']
                # df['high'] = df['high'] * df['qfq_factor']
                # df['low'] = df['low'] * df['qfq_factor']
                # df['close'] = df['close'] * df['qfq_factor']
                # df['volume'] = df['volume'] * df['qfq_factor']
                self.log.error('前复权不常用，计算不方便，有需要自己计算')
                return None

            if fq == 'hfq':
                df['open'] = df['open'] * df['hfq_factor']
                df['high'] = df['high'] * df['hfq_factor']
                df['low'] = df['low'] * df['hfq_factor']
                df['close'] = df['close'] * df['hfq_factor']
                df['volume'] = df['volume'] * df['hfq_factor']

        if proj_tmp is not None:
            df = df[proj_tmp]
        self.log.debug('加载日线数据成功 size={}'.format(df.shape[0]))
        return df

    async def save_stock_daily(self, data: pd.DataFrame) -> List[str]:
        """
        :param code:
        :param data: DataFrame([code,trade_date,open,high,low,close,vol,amt,adj_factor])
        :return: None/list[_id]
        """
        count = data.shape[0] if data is not None else 0
        inserted_ids = []
        self.log.debug('保存日线数据, count = {} ...'.format(count))
        if count > 0:
            inserted_ids = await self.do_insert(coll=self.stock_daily, data=data)
        self.log.debug('保存日线数据成功, size = {}'.format(len(inserted_ids) if inserted_ids is not None else 0))
        return inserted_ids

    async def load_stock_index(self, **kwargs) -> Optional[pd.DataFrame]:
        """
        :param code:
        :param kwargs:  filter=None, projection=None, skip=0, limit=0, sort=None, to_frame=True
        :return: DataFrame([code,trade_date,open,high,low,close,vol,amount])
        """
        self.log.debug('加载股票指标, kwargs={}'.format(kwargs))
        df = await self.do_load(self.stock_index, **kwargs)
        self.log.debug('加载日线数据成功 size={}'.format(df.shape[0] if df is not None else 0))
        return df

    async def save_stock_index(self, data: pd.DataFrame) -> List[str]:
        """
        :param code:
        :param data: DataFrame([code,trade_date,open,high,low,close,vol,amt,adj_factor])
        :return: None/list[_id]
        """
        count = data.shape[0] if data is not None else 0
        inserted_ids = []
        self.log.debug('保存股票指标数据, count = {} ...'.format(count))
        if count > 0:
            inserted_ids = await self.do_insert(coll=self.stock_index, data=data)
        self.log.debug('保存股票指标数据成功, size = {}'.format(len(inserted_ids) if inserted_ids is not None else 0))
        return inserted_ids

    async def load_stock_fq_factor(self, **kwargs) -> Optional[pd.DataFrame]:
        """
        :param kwargs:  filter=None, projection=None, skip=0, limit=0, sort=None, to_frame=True
        :return: DataFrame([code,trade_date,open,high,low,close,vol,amount])
        """
        self.log.debug('加载股票复权因子, kwargs={}'.format(kwargs))
        df = await self.do_load(self.stock_fq_factor, **kwargs)
        self.log.debug('加载复权因子成功 size={}'.format(df.shape[0] if df is not None else 0))
        return df

    async def save_stock_fq_factor(self, data: pd.DataFrame) -> List[str]:
        """
        :param code:
        :param data: DataFrame([code,trade_date,open,high,low,close,vol,amt,adj_factor])
        :return: None/list[_id]
        """
        count = data.shape[0] if data is not None else 0
        inserted_ids = []
        self.log.debug('保存复权因子数据, count = {} ...'.format(count))
        if count > 0:
            inserted_ids = await self.do_insert(coll=self.stock_fq_factor, data=data)
        self.log.debug('保存复权因子数据成功, size = {}'.format(len(inserted_ids) if inserted_ids is not None else 0))
        return inserted_ids

    async def load_index_info(self, **kwargs) -> Optional[pd.DataFrame]:
        """
        指数基本信息
        :return: None/DataFrame([code,name,market,category,index_type,exp_date])
        """
        self.log.debug('加载大盘指数列表, kwargs={} ...'.format(kwargs))
        df = await self.do_load(self.index_info, **kwargs)
        self.log.debug('加载大盘指数列表成功 size={}'.format(df.shape[0] if df is not None else 0))
        return df

    async def save_index_info(self, data: pd.DataFrame) -> List[str]:
        """
        :param data: DataFrame([code,name,market,category,index_type,exp_date])
        :return: None/list[_id]
        """
        count = data.shape[0] if data is not None else 0
        inserted_ids = []
        self.log.debug('保存大盘指数列表, count = {} ...'.format(count))
        if count > 0:
            inserted_ids = await self.do_insert(coll=self.index_info, data=data)
        self.log.debug('保存大盘指数列表成功, size = {}'.format(len(inserted_ids) if inserted_ids is not None else 0))
        return inserted_ids

    async def load_index_daily(self, **kwargs) -> Optional[pd.DataFrame]:
        """

        :param code:
        :param kwargs:
        :return: None/DataFrame([code,trade_date,open,high,low,close,volume,amount])
        """
        self.log.debug('加载大盘日线数据, kwargs={} ...'.format(kwargs))
        df = await self.do_load(self.index_daily, **kwargs)
        self.log.debug('加载大盘日线数据成功 size={}'.format(df.shape[0] if df is not None else 0))
        return df

    async def save_index_daily(self, data: pd.DataFrame) -> List[str]:
        """
        :param code:
        :param data:  DataFrame([code,trade_date,open,high,low,close,vol,amt])
        :return: None/list[_id]
        """
        count = data.shape[0] if data is not None else 0
        inserted_ids = []
        self.log.debug('保存大盘日K数据, count = {} ...'.format(count))
        if count > 0:
            inserted_ids = await self.do_insert(coll=self.index_daily, data=data)
        self.log.debug('保存大盘日K数据成功, size = {}'.format(len(inserted_ids) if inserted_ids is not None else 0))
        return inserted_ids

    async def load_stock_north_south_flow(self, **kwargs) -> Optional[pd.DataFrame]:
        """
        kwargs参数同pymongo参数, 另外增加to_frame
        :param kwargs:  filter=None, projection=None, skip=0, limit=0, sort=None, to_frame=True
        :return: DataFrame
        """
        self.log.debug('加载北向资金信息, kwargs={} ...'.format(kwargs))
        df = await self.do_load(self.stock_ns_flow, **kwargs)
        self.log.debug('加载北向资金信息成功 size={}'.format(df.shape[0] if df is not None else 0))
        return df

    async def save_stock_north_south_flow(self, data: pd.DataFrame) -> List[str]:
        """
        :param data: DataFrame[code name]
        :return: list[_id]
        """
        count = data.shape[0] if data is not None else 0
        inserted_ids = []
        self.log.debug('保存北向资金信息, count = {} ...'.format(count))
        if count > 0:
            inserted_ids = await self.do_insert(coll=self.stock_ns_flow, data=data)
        self.log.debug('保存北向资金信息成功, size = {}'.format(len(inserted_ids) if inserted_ids is not None else 0))
        return inserted_ids

    async def load_stock_his_divend(self, **kwargs) -> Optional[pd.DataFrame]:
        """
        kwargs参数同pymongo参数, 另外增加to_frame
        :param kwargs:  filter=None, projection=None, skip=0, limit=0, sort=None, to_frame=True
        :return: DataFrame
        """
        self.log.debug('加载历史分红信息, kwargs={} ...'.format(kwargs))
        df = await self.do_load(self.stock_his_divend, **kwargs)
        self.log.debug('加载历史分红信息成功 size={}'.format(df.shape[0] if df is not None else 0))
        return df

    async def save_stock_his_divend(self, data: pd.DataFrame) -> List[str]:
        """
        :param data: DataFrame[code name]
        :return: list[_id]
        """
        count = data.shape[0] if data is not None else 0
        inserted_ids = []
        self.log.debug('保存历史分红信息, count = {} ...'.format(count))
        if count > 0:
            inserted_ids = await self.do_insert(coll=self.stock_his_divend, data=data)
        self.log.debug('保存历史分红信息成功, size = {}'.format(len(inserted_ids) if inserted_ids is not None else 0))
        return inserted_ids

    async def load_sw_index_info(self, **kwargs) -> Optional[pd.DataFrame]:
        """
        kwargs参数同pymongo参数, 另外增加to_frame
        :param kwargs:  filter=None, projection=None, skip=0, limit=0, sort=None, to_frame=True
        :return: DataFrame
        """
        self.log.debug('加载申万一级行业信息, kwargs={} ...'.format(kwargs))
        df = await self.do_load(self.sw_index_info, **kwargs)
        self.log.debug('加载申万一级行业信息成功 size={}'.format(df.shape[0] if df is not None else 0))
        return df

    async def save_sw_index_info(self, data: pd.DataFrame) -> List[str]:
        """
        :param data: DataFrame[code name]
        :return: list[_id]
        """
        count = data.shape[0] if data is not None else 0
        inserted_ids = []
        self.log.debug('保存申万一级行业信息, count = {} ...'.format(count))
        if count > 0:
            inserted_ids = await self.do_insert(coll=self.sw_index_info, data=data)
        self.log.debug('保存申万一级行业信息成功, size = {}'.format(len(inserted_ids) if inserted_ids is not None else 0))
        return inserted_ids

    async def load_stock_margin(self, **kwargs) -> Optional[pd.DataFrame]:
        """
        :param kwargs:  filter=None, projection=None, skip=0, limit=0, sort=None, to_frame=True
        :return: DataFrame
        """
        self.log.debug('加载股票融资融券数据, kwargs={}'.format(kwargs))
        df = await self.do_load(self.stock_margin, **kwargs)
        self.log.debug('加载股票融资融券数据成功 size={}'.format(df.shape[0] if df is not None else 0))
        return df

    async def save_stock_margin(self, data: pd.DataFrame) -> List[str]:
        """
        :param data: DataFrame()
        :return: None/list[_id]
        """
        count = data.shape[0] if data is not None else 0
        inserted_ids = []
        self.log.debug('融资融券数据, count = {} ...'.format(count))
        if count > 0:
            inserted_ids = await self.do_insert(coll=self.stock_margin, data=data)
        self.log.debug('保存融资融券数据成功, size = {}'.format(len(inserted_ids) if inserted_ids is not None else 0))
        return inserted_ids


if __name__ == '__main__':
    from bbq.common import run_until_complete
    from datetime import datetime


    async def my_count(db):
        # count = await db.stock_info.count_documents({})
        # print('count={}'.format(count))
        # await db.do_update_many(coll=db.stock_fq_factor,
        #                         filter={'code': 'sh600027'},
        #                         update={'sync_date': datetime.now()}, upsert=False)
        pass


    db = StockDB(uri='mongodb://localhost:27017/')
    db.init()
    run_until_complete(
        my_count(db)
    )
