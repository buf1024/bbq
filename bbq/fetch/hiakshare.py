from akshare import *

import pandas as pd
import requests
from tqdm import tqdm
from io import BytesIO
import time
from akshare.utils import demjson


# patch
# stock/stock_info.py
def stock_info_sz_name_code(indicator: str = "主板") -> pd.DataFrame:
    """
    深圳证券交易所-股票列表
    http://www.szse.cn/market/companys/company/index.html
    :param indicator: choice of {"A股列表", "B股列表", "上市公司列表", "主板", "中小企业板", "创业板"}
    :type indicator: str
    :return: 指定 indicator 的数据
    :rtype: pandas.DataFrame
    """
    url = "http://www.szse.cn/api/report/ShowReport"
    if indicator in {"A股列表", "B股列表"}:
        indicator_map = {"A股列表": "tab1", "B股列表": "tab2", "AB股列表": "tab3"}
        params = {
            "SHOWTYPE": "xlsx",
            "CATALOGID": "1110",
            "TABKEY": indicator_map[indicator],
            "random": "0.6935816432433362",
        }
        r = requests.get(url, params=params)
        temp_df = pd.read_excel(BytesIO(r.content))
        if indicator == "A股列表":
            temp_df["A股代码"] = temp_df["A股代码"].astype(str).str.zfill(6)
            return temp_df
        else:
            temp_df["A股代码"] = temp_df["A股代码"].fillna(0).astype(int).astype(str).str.zfill(6).replace("000000", "-")
            return temp_df
    else:
        indicator_map = {"上市公司列表": "tab1", "主板": "tab2", "中小企业板": "tab3", "创业板": "tab4"}
        params = {
            "SHOWTYPE": "xlsx",
            "CATALOGID": "1110x",
            "TABKEY": indicator_map[indicator],
            "random": "0.6935816432433362",
        }
        r = requests.get(url, params=params)
        temp_df = pd.read_excel(BytesIO(r.content))
        temp_df["A股代码"] = temp_df["A股代码"].fillna(0).astype(int).astype(str).str.zfill(6).replace("000000", "-")
        return temp_df


# stock_feature/stock_szse_margin.py
def stock_margin_detail_sse(date: str = "20210205") -> pd.DataFrame:
    """
    上海证券交易所-融资融券数据-融资融券明细
    http://www.sse.com.cn/market/othersdata/margin/detail/
    :param date: 交易日期
    :type date: str
    :return: 融资融券明细
    :rtype: pandas.DataFrame
    """
    url = "http://query.sse.com.cn/marketdata/tradedata/queryMargin.do"
    params = {
        "isPagination": "true",
        "tabType": "mxtype",
        "detailsDate": date,
        "stockCode": "",
        "beginDate": "",
        "endDate": "",
        "pageHelp.pageSize": "5000",
        "pageHelp.pageCount": "50",
        "pageHelp.pageNo": "1",
        "pageHelp.beginPage": "1",
        "pageHelp.cacheSize": "1",
        "pageHelp.endPage": "21",
        "_": "1612773448860",
    }
    headers = {
        "Referer": "http://www.sse.com.cn/",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/88.0.4324.150 Safari/537.36",
    }
    r = requests.get(url, params=params, headers=headers)
    data_json = r.json()
    temp_df = pd.DataFrame(data_json["result"])
    if not temp_df.empty:
        temp_df.columns = [
            "_",
            "信用交易日期",
            "融券偿还量",
            "融券卖出量",
            "融券余量",
            "_",
            "_",
            "融资偿还额",
            "融资买入额",
            "_",
            "融资余额",
            "标的证券简称",
            "标的证券代码",
        ]
        temp_df = temp_df[
            [
                "信用交易日期",
                "标的证券代码",
                "标的证券简称",
                "融资余额",
                "融资买入额",
                "融资偿还额",
                "融券余量",
                "融券卖出量",
                "融券偿还量",
            ]
        ]
    return temp_df


def stock_margin_detail_szse(date: str = "20210728") -> pd.DataFrame:
    """
    深证证券交易所-融资融券数据-融资融券明细
    c
    :param date: 交易日期
    :type date: str
    :return: 融资融券明细
    :rtype: pandas.DataFrame
    """
    url = "http://www.szse.cn/api/report/ShowReport/data"
    params = {
        "SHOWTYPE": "JSON",
        "CATALOGID": "1837_xxpl",
        "txtDate": "-".join([date[:4], date[4:6], date[6:]]),
        "tab2PAGENO": "1",
        "random": "0.7425245522795993",
    }
    headers = {
        "Referer": "http://www.szse.cn/disclosure/margin/margin/index.html",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/88.0.4324.150 Safari/537.36",
    }
    r = requests.get(url, params=params, headers=headers)
    data_json = r.json()
    total_page = data_json[1]["metadata"]["pagecount"]
    big_df = pd.DataFrame()
    for page in tqdm(range(1, total_page + 1), leave=False):
        params.update({"tab2PAGENO": page})
        r = requests.get(url, params=params, headers=headers)
        data_json = r.json()
        temp_df = pd.DataFrame(data_json[1]["data"])
        big_df = big_df.append(temp_df, ignore_index=True)
    if not big_df.empty:
        big_df.columns = [
            "证券代码",
            "证券简称",
            "融资买入额",
            "融资余额",
            "融券卖出量",
            "融券余量",
            "融券余额",
            "融资融券余额",
        ]
        big_df["证券简称"] = big_df["证券简称"].str.replace("&nbsp;", "")
        big_df["融资买入额"] = big_df["融资买入额"].str.replace(",", "")
        big_df["融资买入额"] = pd.to_numeric(big_df["融资买入额"])
        big_df["融资余额"] = big_df["融资余额"].str.replace(",", "")
        big_df["融资余额"] = pd.to_numeric(big_df["融资余额"])
        big_df["融券卖出量"] = big_df["融券卖出量"].str.replace(",", "")
        big_df["融券卖出量"] = pd.to_numeric(big_df["融券卖出量"])
        big_df["融券余量"] = big_df["融券余量"].str.replace(",", "")
        big_df["融券余量"] = pd.to_numeric(big_df["融券余量"])
        big_df["融券余额"] = big_df["融券余额"].str.replace(",", "")
        big_df["融券余额"] = pd.to_numeric(big_df["融券余额"])
        big_df["融资融券余额"] = big_df["融资融券余额"].str.replace(",", "")
        big_df["融资融券余额"] = pd.to_numeric(big_df["融资融券余额"])
    return big_df

def fund_em_etf_fund_info(
        fund: str = "511280", start_date: str = "20000101", end_date: str = "20500101"
) -> pd.DataFrame:
    """
    东方财富网站-天天基金网-基金数据-场内交易基金-历史净值明细
    http://fundf10.eastmoney.com/jjjz_511280.html
    :param fund: 场内交易基金代码, 可以通过 fund_em_etf_fund_daily 来获取
    :type fund: str
    :param start_date: 开始统计时间
    :type start_date: str
    :param end_date: 结束统计时间
    :type end_date: str
    :return: 东方财富网站-天天基金网-基金数据-场内交易基金-历史净值明细
    :rtype: pandas.DataFrame
    """
    url = "http://api.fund.eastmoney.com/f10/lsjz"
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/80.0.3987.149 Safari/537.36",
        "Referer": f"http://fundf10.eastmoney.com/jjjz_{fund}.html",
    }
    params = {
        "callback": "jQuery183023608994033331676_1588250653363",
        "fundCode": fund,
        "pageIndex": "1",
        "pageSize": "10000",
        "startDate": "-".join([start_date[:4], start_date[4:6], start_date[6:]]),
        "endDate": "-".join([end_date[:4], end_date[4:6], end_date[6:]]),
        "_": round(time.time() * 1000),
    }
    r = requests.get(url, params=params, headers=headers)
    text_data = r.text
    data_json = demjson.decode(text_data[text_data.find("{") : -1])
    temp_df = pd.DataFrame(data_json["Data"]["LSJZList"])
    if not temp_df.empty:
        temp_df.columns = [
            "净值日期",
            "单位净值",
            "累计净值",
            "_",
            "_",
            "_",
            "日增长率",
            "申购状态",
            "赎回状态",
            "_",
            "_",
            "_",
            "_",
        ]
        temp_df = temp_df[["净值日期", "单位净值", "累计净值", "日增长率", "申购状态", "赎回状态"]]
        temp_df["净值日期"] = pd.to_datetime(temp_df["净值日期"]).dt.date
        temp_df["单位净值"] = pd.to_numeric(temp_df["单位净值"])
        temp_df["累计净值"] = pd.to_numeric(temp_df["累计净值"])
        temp_df["日增长率"] = pd.to_numeric(temp_df["日增长率"])
    return temp_df


