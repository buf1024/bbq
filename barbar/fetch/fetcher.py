# from typing import List, Dict, Any, Optional
# import pandas as pd
#
#
# def get_trade_cal(start: str = None, end: str = None) -> Optional[pd.DataFrame]:
#     """
#     交易日历
#     :param end 格式 'yyyymmdd'
#     :param start 格式 'yyyymmdd'
#     :return: DataFrame[cal_date,is_open]
#     """
#     raise NotImplementedError
#
#
# def get_code_list(codes: List[str] = None) -> Optional[pd.DataFrame]:
#     """
#     股票列表
#     :return: None / DataFrame[code name]
#     """
#     raise NotImplementedError
#
#
# def get_index_list(codes: List[str] = None) -> Optional[pd.DataFrame]:
#     """
#     指数基本信息
#     :return: None/DataFrame([code,name])
#     """
#     raise NotImplementedError
#
#
# def get_bar(code: str, frequency: str = 'D', start: str = None, end: str = None) -> Optional[pd.DataFrame]:
#     """
#     :param end:
#     :param start:
#     :param frequency:
#     :param code: code=xxx.sh/sz
#     :return: DataFrame([code,trade_date,open,high,low,close,vol,amount])
#     """
#     raise NotImplementedError
#
#
# def get_adj_factor(code: str, start: str = None, end: str = None) -> Optional[pd.DataFrame]:
#     """
#     :param end:
#     :param start:
#     :param code: code=xxx.sh/sz
#     :return: DataFrame([code,trade_date,adj_factor])
#     """
#     raise NotImplementedError
#
#
# def get_index_bar(code: str, frequency: str = 'D', start: str = None, end: str = None) -> Optional[pd.DataFrame]:
#     """
#     :param end:
#     :param start:
#     :param frequency:
#     :param code: code=xxx.sh/sz
#     :return: DataFrame([code,trade_date,open,high,low,close,vol,amount])
#     """
#     raise NotImplementedError
#
#
# def get_rt_quot(codes: List[str]) -> Dict[str, Dict[str, Any]]:
#     """
#
#     :param codes:
#     :return:
#     [code: {
#         name=xxx,
#         open=xx,pre_close=xx,now=xx,high=xx,low=xx,buy=xx,sell=xx,
#         vol=xx, amount=xx, # 累计成交量、成交额
#         bid=[(bid1_vol, bid1_price), ...], ask=[(ask1_vol, ask1_price), ...],
#         date=yyyymmdd,time=hh:mm:ss}, ...]/None,
#     """
#     raise NotImplementedError
