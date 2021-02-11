import pandas as pd
from typing import Sequence, ClassVar
from pyecharts.commons.utils import JsCode
from pyecharts import options as opts
from pyecharts.charts import *
import talib
from pyecharts.charts.chart import Chart
from .tools import linear_fitting


def up_color() -> str:
    return '#F11300'


def down_color() -> str:
    return '#00A800'


def kline_tooltip_fmt_func():
    return JsCode("""function(obj){
                    function tips_str(pre, now, tag, field, unit) {
                        var tips = tag + ':&nbsp;&nbsp;&nbsp;&nbsp;';
                        var span = '';

                        if (field == 'open' || field == 'close' ||
                            field == 'low' || field == 'high') {
                            var fixed = 3;
                            if (now['code'].startsWith('s')) {
                                fixed = 2;
                            }
                            var chg = (now[field] - now['open']).toFixed(fixed);
                            var rate = (chg * 100 / now['open']).toFixed(2);
                            if (pre != null) {
                                chg = (now[field] - pre['close']).toFixed(fixed);
                                rate = (chg * 100 / pre['close']).toFixed(2);
                            }
                            if (rate >= 0) {
                                span = '<span style="color: ' + colors[0] + ';">' + now[field].toFixed(fixed) + '&nbsp;&nbsp;(' + rate + '%,&nbsp;' + chg + ')</span><br>';
                            } else {
                                span = '<span style="color: ' + colors[1] + ';">' + now[field].toFixed(fixed) + '&nbsp;&nbsp;(' + rate + '%,&nbsp;' + chg + ')</span><br>';
                            }
                        } else {
                            if (field == 'volume') {
                                span = (now[field] / 1000000.0).toFixed(2) + '&nbsp;万手<br>';
                            }
                            if (field == 'turnover') {
                                span = now[field] + '%<br>'
                            }
                        }
                        return tips + span;

                    }

                    var pre_data = null;
                    var now_data = kdata[trade_date[obj.dataIndex]];
                    if (obj.dataIndex > 0) {
                        pre_data = kdata[trade_date[obj.dataIndex-1]];
                    }
                    var title = now_data['code'] + '&nbsp;&nbsp;' + trade_date[obj.dataIndex] + '<br><br>';

                    if ('name' in now_data) {
                        title = now_data['name'] + '(' + now_data['code'] + ')&nbsp;&nbsp;' + trade_date[obj.dataIndex] + '</div><br>';
                    }

                    return title + 
                        tips_str(pre_data, now_data, '开盘', 'open') + 
                        tips_str(pre_data, now_data, '最高', 'high') + 
                        tips_str(pre_data, now_data, '最低', 'low') + 
                        tips_str(pre_data, now_data, '收盘', 'close') + 
                        tips_str(pre_data, now_data, '成交', 'volume') + 
                        tips_str(pre_data, now_data, '换手', 'turnover');
                     }""")


def kline_orig_data(df):
    origData = df[:]
    origData['trade_date'] = origData['trade_date'].apply(lambda d: d.strftime('%Y/%m/%d')[2:])
    origData.index = origData['trade_date']
    origData.fillna('-', inplace=True)
    return origData.to_dict('index')


def plot_overlap(main_chart, *overlap):
    for sub_chart in overlap:
        main_chart.overlap(sub_chart)
    return main_chart


def plot_chart(chart_cls: ClassVar, x_index: list, y_data: list, title: str,
               show_label=False, symbol=None, symbol_rotate=None, with_global_opts: bool = False,
               *overlap):
    chart = chart_cls()
    chart.add_xaxis(xaxis_data=x_index)
    if chart_cls == Line:
        if symbol is None:
            symbol = 'none'
        chart.add_yaxis(series_name=title, y_axis=y_data,
                        label_opts=opts.LabelOpts(is_show=show_label),
                        is_smooth=True, symbol=symbol)
    else:
        chart.add_yaxis(series_name=title, y_axis=y_data,
                        label_opts=opts.LabelOpts(is_show=show_label),
                        symbol=symbol, symbol_rotate=symbol_rotate, symbol_size=8)
    if with_global_opts:
        chart.set_global_opts(xaxis_opts=opts.AxisOpts(is_scale=True),
                              yaxis_opts=opts.AxisOpts(
                                  is_scale=True,
                                  splitarea_opts=opts.SplitAreaOpts(
                                      is_show=True, areastyle_opts=opts.AreaStyleOpts(opacity=1)
                                  ),
                              ),
                              datazoom_opts=[opts.DataZoomOpts(pos_bottom="-2%", filter_mode='none')],
                              title_opts=opts.TitleOpts(title=title))
    return plot_overlap(chart, *overlap)


def plot_kline(data: pd.DataFrame, *, title: str = '日线', with_global_opts: bool = True,
               overlap: Sequence = ('MA5', 'MA10', 'MA20')):
    kdata = list(zip(data['open'], data['close'], data['low'], data['high']))
    trade_date = [d.strftime('%Y/%m/%d')[2:] for d in data['trade_date']]

    kline = Kline()
    kline.add_js_funcs('var kdata={}'.format(kline_orig_data(data)))
    kline.add_js_funcs('var trade_date={}'.format(trade_date))
    kline.add_js_funcs('var colors=["{}", "{}"]'.format(up_color(), down_color()))
    kline.add_xaxis(trade_date)
    kline.add_yaxis(series_name=title, y_axis=kdata,
                    itemstyle_opts=opts.ItemStyleOpts(
                        color=up_color(),
                        color0=down_color(),
                    ),
                    tooltip_opts=opts.TooltipOpts(
                        formatter=kline_tooltip_fmt_func())
                    )
    if with_global_opts:
        kline.set_global_opts(xaxis_opts=opts.AxisOpts(is_scale=True),
                              yaxis_opts=opts.AxisOpts(
                                  is_scale=True,
                                  splitarea_opts=opts.SplitAreaOpts(
                                      is_show=True, areastyle_opts=opts.AreaStyleOpts(opacity=1)
                                  ),
                              ),
                              datazoom_opts=[opts.DataZoomOpts(pos_bottom="-2%", filter_mode='none')],
                              title_opts=opts.TitleOpts(title=title))

    if overlap is not None:
        charts = []
        for typ in overlap:
            chart = None
            if isinstance(typ, str):
                typ = typ.upper()
                if typ.startswith('MA'):
                    tm = int(typ[2:])
                    ma = talib.MA(data['close'], timeperiod=tm)
                    ma = [round(v, 3) for v in ma]
                    chart = Line()
                    chart.add_xaxis(trade_date)
                    chart.add_yaxis(series_name=typ, y_axis=ma,
                                    label_opts=opts.LabelOpts(is_show=False),
                                    is_smooth=True,
                                    symbol='none')
            if isinstance(typ, Chart):
                chart = typ
            charts.append(chart)
        kline = plot_overlap(kline, *charts)
    return kline


def my_plot(data: pd.DataFrame, ma=(3, 6)):
    a, b, score, x_index, y_index = linear_fitting(data)
    line = plot_chart(Line, x_index, y_index, '拟合({}, {})'.format(round(a, 2), round(score, 2)))
    overlap = ['MA' + str(p) for p in ma]
    overlap.append(line)
    return plot_kline(data, overlap=overlap)
