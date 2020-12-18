import pandas as pd
from datetime import datetime
import mplfinance as mpf

__all__ = ['to_trade_date', 'to_mpf_df', 'plot']


def to_trade_date(trade_date: datetime) -> datetime:
    return datetime(year=trade_date.year, month=trade_date.month, day=trade_date.day)


def to_mpf_df(df: pd.DataFrame) -> pd.DataFrame:
    columns = {}
    for col in df.columns:
        if col in ['open', 'high', 'close', 'low', 'volume']:
            columns[col] = col.capitalize()
    if len(columns) > 0:
        df = df.rename(columns=columns)
    if 'trade_date' in df.columns:
        df = df.rename(columns={'trade_date': 'Date'})
        df.index = df['Date']
    return df


def plot(df: pd.DataFrame, typ='candle', base_style='checkers', **kwargs):
    my_color = mpf.make_marketcolors(up='red', down='green', edge='black', wick='i',
                                     volume={'up': 'red', 'down': 'green'})

    my_style = mpf.make_mpf_style(base_mpf_style=base_style, marketcolors=my_color,
                                  gridaxis='both', gridstyle='-.', y_on_right=True)

    df = to_mpf_df(df)
    mpf.plot(df, type=typ, style=my_style, **kwargs)
