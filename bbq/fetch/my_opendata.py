import opendatatools.stock as stock
import pandas as pd

if __name__ == '__main__':
    pd.set_option('display.max_column', None)
    # 获取实时行情
    # df, msg = stock.get_quote('600000.SH,000002.SZ')
    # print(df)
    # print(df.columns)

    # 获取实时K线
    # df, msg = stock.get_kline('000002.SZ', '2020-11-11', '1m')
    # print(df)
    # print(df.columns)

    # 获取日线数据
    df, msg = stock.get_daily('300999.SZ', start_date='2020-11-11', end_date='2020-11-13')
    print(df)
    print(df.columns)

    # 获取复权因子 -- 无效
    # df, msg = stock.get_adj_factor('000001.SZ')
    # print(df)
    # print(df.columns)

