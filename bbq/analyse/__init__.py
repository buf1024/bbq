import pandas as pd
from datetime import datetime
from talib.abstract import *
import numpy as np
from pyecharts import options as opts
from pyecharts.charts import *
import talib

__all__ = ['to_trade_date', 'plot']

# talib.MA()
# opts.TooltipOpts


def to_trade_date(trade_date: datetime) -> datetime:
    return datetime(year=trade_date.year, month=trade_date.month, day=trade_date.day)


def plot(df: pd.DataFrame, *, is_notebook=True, render_html='render_html.html', **kwargs):
    k_data = list(zip(df['open'], df['close'], df['low'], df['high']))
    trade_date = [d.strftime('%Y-%m-%d') for d in df['trade_date']]
    kline = Kline()
    kline.add_xaxis(xaxis_data=trade_date)
    kline.add_yaxis(
        series_name='日K',
        y_axis=k_data,
        itemstyle_opts=opts.ItemStyleOpts(
            color="#ef232a",
            color0="#14b143",
            border_color="#ef232a",
            border_color0="#14b143",
        ),
        markpoint_opts=opts.MarkPointOpts(
            data=[
                opts.MarkPointItem(type_="max", name="最大值"),
                opts.MarkPointItem(type_="min", name="最小值"),
            ]
        ),
        markline_opts=opts.MarkLineOpts(
            label_opts=opts.LabelOpts(
                position="middle", color="blue", font_size=15
            ),
            data=[
                {
                    "xAxis": '2020-12-02',
                    "yAxis": 3.90,
                    "value": 'Buy',
                }
            ],
            symbol=["circle", "none"],
        ),
    )
    kline.set_series_opts(
        markarea_opts=opts.MarkAreaOpts(is_silent=True, data=[
            {
                "xAxis": '2020-12-02',
                "yAxis": 3.90,
                "value": 'Buy',
            }
        ])
    )
    kline.set_global_opts(
        title_opts=opts.TitleOpts(title="K线图", pos_left="0"),
        xaxis_opts=opts.AxisOpts(
            type_="category",
            is_scale=True,
            boundary_gap=False,
            axisline_opts=opts.AxisLineOpts(is_on_zero=False),
            splitline_opts=opts.SplitLineOpts(is_show=False),
            split_number=20,
            min_="dataMin",
            max_="dataMax",
        ),
        yaxis_opts=opts.AxisOpts(
            is_scale=True, splitline_opts=opts.SplitLineOpts(is_show=True)
        ),
        tooltip_opts=opts.TooltipOpts(trigger="axis", axis_pointer_type="line"),
        datazoom_opts=[
            opts.DataZoomOpts(
                is_show=False, type_="inside", xaxis_index=[0, 0], range_end=100
            ),
            opts.DataZoomOpts(
                is_show=True, xaxis_index=[0, 1], pos_top="97%", range_end=100
            ),
            opts.DataZoomOpts(is_show=False, xaxis_index=[0, 2], range_end=100),
        ],
    )
    ma_data = MA(df)
    ma_data = ma_data.values
    kline_line = Line()
    kline_line.add_xaxis(xaxis_data=trade_date)
    kline_line.add_yaxis(
        series_name="MA",
        y_axis=ma_data,
        is_smooth=True,
        linestyle_opts=opts.LineStyleOpts(opacity=0.5),
        label_opts=opts.LabelOpts(is_show=False),
    )
    kline_line.set_global_opts(
        xaxis_opts=opts.AxisOpts(
            type_="category",
            grid_index=1,
            axislabel_opts=opts.LabelOpts(is_show=False),
        ),
        yaxis_opts=opts.AxisOpts(
            grid_index=1,
            split_number=3,
            axisline_opts=opts.AxisLineOpts(is_on_zero=False),
            axistick_opts=opts.AxisTickOpts(is_show=False),
            splitline_opts=opts.SplitLineOpts(is_show=False),
            axislabel_opts=opts.LabelOpts(is_show=True),
        ),
    )

    # kline.overlap(kline_line)

    return kline

    # if is_notebook:
    #     kline.render_notebook()
    # else:
    #     kline.render(render_html)


if __name__ == '__main__':
    # import pandas as pd
    # from datetime import datetime, timedelta
    # import mplfinance as mpf
    from bbq.data.stockdb import StockDB
    from bbq.common import run_until_complete


    async def test():
        db = StockDB()
        db.init()

        data = await db.load_stock_daily(filter={'code': 'sz000001'}, limit=60, sort=[('trade_date', -1)])
        data = data[::-1]

        plot(data)


    run_until_complete(test())
