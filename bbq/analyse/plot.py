import pandas as pd
from typing import Sequence, ClassVar
from pyecharts.commons.utils import JsCode
from pyecharts import options as opts
from pyecharts.charts import *
import talib
from pyecharts.charts.chart import Chart
from pyecharts.globals import SymbolType


def hi_mark() -> str:
    return 'image://data:image/svg+xml;base64,PHN2ZyB0PSIxNjMyMjMxMjI2MjU5IiBjbGFzcz0iaWNvbiIgdmlld0JveD0iMCAwIDEwMjQgMTAyNCIgdmVyc2lvbj0iMS4xIiB4bWxucz0iaHR0cDovL3d3dy53My5vcmcvMjAwMC9zdmciIHAtaWQ9IjMyMDQiIHdpZHRoPSIyMDAiIGhlaWdodD0iMjAwIj48cGF0aCBkPSJNNzQ0LjYzIDY0OC4yNjNsLTQ2MC41MjcgMCAyMzAuMjY1IDMxMS4wODV6TTY0MC42NjIgNDMwLjI0bDAgMTgwLjgtMjU1LjI0NyAwIDAtMTgwLjggMjU1LjI0NyAwek02NDAuNjYyIDI0NC4xMjJsMCAxMjIuMzA3LTI1NS4yNDcgMCAwLTEyMi4zMDcgMjU1LjI0NyAwek02NDAuNjYyIDYzLjMyM2wwIDkwLjM5OS0yNTUuMjQ3IDAgMC05MC4zOTkgMjU1LjI0NyAweiIgcC1pZD0iMzIwNSIgZmlsbD0iI0YxMTMwMCI+PC9wYXRoPjwvc3ZnPg=='


def up_color() -> str:
    return '#F11300'


def down_color() -> str:
    return '#00A800'


def mix_color() -> str:
    return '#F09A00'


def ma_color(ma: str) -> str:
    tab = {
        'ma5': '#1D1D1D',
        'ma10': '#F09A00',
        'ma20': '#FF476C',
        'ma30': '#4DC331'
    }
    ma = ma.lower()
    if ma not in tab:
        return ''
    return tab[ma]


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
    orig_data = df[:]
    orig_data['trade_date'] = orig_data['trade_date'].apply(lambda d: d.strftime('%Y/%m/%d')[2:])
    orig_data.index = orig_data['trade_date']
    orig_data.fillna('-', inplace=True)
    return orig_data.to_dict('index')


def plot_overlap(main_chart, *overlap):
    for sub_chart in overlap:
        main_chart.overlap(sub_chart)
    return main_chart


def plot_chart(chart_cls: ClassVar, x_index: list, y_data: list, title: str = '',
               show_label: bool = False, symbol=None,
               *overlap, **options):
    chart = chart_cls()
    chart.add_xaxis(xaxis_data=x_index)
    if symbol is None:
        symbol = 'none'
    chart.add_yaxis(series_name=title, y_axis=y_data,
                    label_opts=opts.LabelOpts(is_show=show_label),
                    symbol=symbol, **options)

    chart.set_global_opts(xaxis_opts=opts.AxisOpts(is_scale=True),
                          yaxis_opts=opts.AxisOpts(
                              is_scale=True,
                              splitarea_opts=opts.SplitAreaOpts(
                                  is_show=True, areastyle_opts=opts.AreaStyleOpts(opacity=1)
                              ),
                          ),
                          title_opts=opts.TitleOpts(title=title))
    return plot_overlap(chart, *overlap)


def plot_volume(data: pd.DataFrame, *, title: str = '成交量', is_grid=False,
                overlap: Sequence = ('ma5', 'ma10'),
                **options):
    trade_date = [d.strftime('%Y/%m/%d')[2:] for d in data['trade_date']]

    bar = Bar()
    bar.add_xaxis(trade_date)
    ydata = [{'name': k, 'value': v} for k, v in zip(trade_date, data['volume'])]

    if not is_grid:
        bar.add_js_funcs('var kdata={}'.format(kline_orig_data(data)))
        bar.add_js_funcs('var trade_date={}'.format(trade_date))
        bar.add_js_funcs('var colors=["{}", "{}"]'.format(up_color(), down_color()))

    bar.add_yaxis(title, ydata, label_opts=opts.LabelOpts(is_show=False),
                  itemstyle_opts=opts.ItemStyleOpts(color=JsCode(
                      """
                          function(params) {
                              var data = kdata[trade_date[params.dataIndex]];
                              if (data['close'] > data['open']) {
                                  return colors[0];
                              } else {
                                  return colors[1];
                              }
                          }
                   """
                  )), category_gap='30%',
                  tooltip_opts=opts.TooltipOpts(formatter=JsCode(
                      '''function (obj){
                        return obj['data']['name'] + '<br>'+(obj['data']['value'] / 10000).toFixed(2) + '万手'
                    }''')),
                  **options)

    max_val, min_val = data['volume'].max(), data['volume'].min()
    max_y, min_y = int(max_val * 1.1), int(min_val * 0.9)
    bar.set_global_opts(yaxis_opts=opts.AxisOpts(
        max_=max_y,
        min_=min_y,
        is_scale=True,
        splitarea_opts=opts.SplitAreaOpts(
            is_show=True, areastyle_opts=opts.AreaStyleOpts(opacity=1)
        ),
        axislabel_opts=opts.LabelOpts(is_show=not is_grid)
    ))

    if overlap is not None:
        charts = []
        for typ in overlap:
            chart = None
            if isinstance(typ, str):
                typ = typ.lower()
                title = typ
                if is_grid:
                    title = None
                if typ.startswith('ma'):
                    tm = int(typ[2:])
                    ma = talib.MA(data['volume'], timeperiod=tm)
                    ma = [round(v, 3) for v in ma]
                    chart = plot_chart(chart_cls=Line, x_index=trade_date, y_data=ma,
                                       is_smooth=True, title=title,
                                       itemstyle_opts=opts.ItemStyleOpts(color=ma_color(typ)))
            if isinstance(typ, Chart):
                chart = typ
            charts.append(chart)
        bar = plot_overlap(bar, *charts)

    return bar


def plot_kline(data: pd.DataFrame, *, title: str = '日线', is_grid=False,
               overlap: Sequence = ('ma5', 'ma10', 'ma20', 'ma30'),
               **options):
    """
    kline 默认标记最高点，最低点。 包括，MA5, MA10, MA20，可随意叠加overlap。
    :param is_grid:
    :param data:
    :param title:
    :param overlap:
    :return:
    """
    kdata = list(zip(data['open'], data['close'], data['low'], data['high']))
    trade_date = [d.strftime('%Y/%m/%d')[2:] for d in data['trade_date']]

    max_val, min_val = data['high'].max(), data['low'].min()
    max_y, min_y = int(max_val * 1.1), int(min_val * 0.9)

    max_trade_date = data[data['high'] == max_val].iloc[0]['trade_date'].strftime('%Y/%m/%d')[2:]
    min_trade_date = data[data['low'] == min_val].iloc[0]['trade_date'].strftime('%Y/%m/%d')[2:]

    kline = Kline()
    if not is_grid:
        kline.add_js_funcs('var kdata={}'.format(kline_orig_data(data)))
        kline.add_js_funcs('var trade_date={}'.format(trade_date))
        kline.add_js_funcs('var colors=["{}", "{}"]'.format(up_color(), down_color()))

    kline.add_xaxis(trade_date)
    kline.add_yaxis(series_name=title, y_axis=kdata,
                    itemstyle_opts=opts.ItemStyleOpts(
                        color=up_color(),
                        color0=down_color(),
                    ),
                    tooltip_opts=opts.TooltipOpts(formatter=kline_tooltip_fmt_func()),
                    **options)

    kline.set_global_opts(xaxis_opts=opts.AxisOpts(is_scale=True),
                          yaxis_opts=opts.AxisOpts(
                              max_=max_y,
                              min_=min_y,
                              is_scale=True,
                              splitarea_opts=opts.SplitAreaOpts(
                                  is_show=True, areastyle_opts=opts.AreaStyleOpts(opacity=1)
                              ),
                          ),
                          legend_opts=opts.LegendOpts(type_='scroll'),
                          title_opts=opts.TitleOpts(title=title))

    scatter_min = plot_chart(chart_cls=Scatter,
                             x_index=[min_trade_date], y_data=[min_val],
                             symbol=SymbolType.ARROW, symbol_size=12,
                             itemstyle_opts=opts.ItemStyleOpts(color=down_color()),
                             tooltip_opts=opts.TooltipOpts(formatter=JsCode(
                                 """function (param){
                                    var obj=param['data'];
                                    return '局部最低价: (' + obj[0] + ', ' + obj[1].toFixed(2) + ')';
                                }"""
                             )))
    scatter_max = plot_chart(chart_cls=Scatter,
                             x_index=[max_trade_date], y_data=[max_val],
                             symbol=SymbolType.ARROW, symbol_size=12, symbol_rotate=180,
                             itemstyle_opts=opts.ItemStyleOpts(color=up_color()),
                             tooltip_opts=opts.TooltipOpts(formatter=JsCode(
                                 """function (param){
                                    var obj=param['data'];
                                    return '局部最高价: (' + obj[0] + ', ' + obj[1].toFixed(2) + ')';
                                }"""
                             )))

    kline = plot_overlap(kline, scatter_min, scatter_max)
    if overlap is not None:
        charts = []
        for typ in overlap:
            chart = None
            if isinstance(typ, str):
                typ = typ.lower()
                if typ.startswith('ma'):
                    tm = int(typ[2:])
                    ma = talib.MA(data['close'], timeperiod=tm)
                    ma = [round(v, 3) for v in ma]
                    chart = plot_chart(chart_cls=Line, x_index=trade_date, y_data=ma,
                                       is_smooth=True, title=typ,
                                       itemstyle_opts=opts.ItemStyleOpts(color=ma_color(typ)))
            if isinstance(typ, Chart):
                chart = typ
            charts.append(chart)
        kline = plot_overlap(kline, *charts)
    return kline


def my_plot(data: pd.DataFrame, marks=None):
    """
    plot图象观察
    :param marks: marks: [{color:xx, data:[{trade_date:.. tip:...}...]}]
    :param data:
    :return:
    """

    tips, scatters = None, []
    if marks is not None:
        tips = {}
        for mark in marks:
            if mark is None and 'color' not in mark and 'data' not in mark:
                continue

            color = mark['color']
            symbol = mark['symbol'] if 'symbol' in mark else SymbolType.ARROW
            x_index, y_index = [], []
            for item in mark['data']:
                if 'trade_date' not in item or 'tip' not in item:
                    continue
                index = item['trade_date'].strftime('%Y/%m/%d')[2:]
                x_index.append(index)
                y_index.append(data[data['trade_date'] == item['trade_date']].iloc[0]['high'])
                tips[index] = item['tip'].replace('\n', '<br>')

            scatter = plot_chart(chart_cls=Scatter,
                                 x_index=x_index, y_data=y_index,
                                 symbol=symbol, symbol_size=15,
                                 itemstyle_opts=opts.ItemStyleOpts(color=color),
                                 tooltip_opts=opts.TooltipOpts(formatter=JsCode(
                                     """function (param){
                                        var obj=param['data'];
                                        return '(' + obj[0] + ', ' + obj[1].toFixed(2) + ')<br>' + tips[obj[0]];
                                    }"""
                                 )))
            scatters.append(scatter)

    overlap = ['ma5', 'ma10', 'ma20', 'ma30']
    if tips is not None and scatters is not None:
        overlap = overlap + scatters

    return plot_grid(data=data, title='日线', overlap=overlap,
                     chart1=plot_volume(data, is_grid=True),
                     jscode='var tips = {}'.format(tips))


def plot_grid(data: pd.DataFrame, title=None, overlap=None, chart1=None, jscode=None):
    """
    第一个为kline，下方有1

    :return:
    """

    if chart1 is None:
        return plot_kline(data=data, title=title)

    kline = plot_kline(data=data, is_grid=True, overlap=overlap)
    kline.set_global_opts(xaxis_opts=opts.AxisOpts(is_show=True, axislabel_opts=opts.LabelOpts(is_show=False)))

    grid = Grid()
    trade_date = [d.strftime('%Y/%m/%d')[2:] for d in data['trade_date']]
    grid.add_js_funcs('var kdata={}'.format(kline_orig_data(data)))
    grid.add_js_funcs('var trade_date={}'.format(trade_date))
    grid.add_js_funcs('var colors=["{}", "{}"]'.format(up_color(), down_color()))
    if jscode is not None:
        if isinstance(jscode, str):
            grid.add_js_funcs(jscode)
        else:
            for code in jscode:
                grid.add_js_funcs(code)

    pos_bottom, pos_top = '30%', '70%'

    kline.set_global_opts(xaxis_opts=opts.AxisOpts(is_show=True, axislabel_opts=opts.LabelOpts(is_show=False)))
    grid.add(kline, grid_opts=opts.GridOpts(pos_bottom=pos_bottom))
    grid.add(chart1, grid_opts=opts.GridOpts(pos_top=pos_top))
    return grid


if __name__ == '__main__':
    from bbq import *
    from datetime import datetime
    from decimal import Decimal


    async def test():
        fund, stock, mysql = default(log_level='error')
        df = mysql.execute("select a.name, b.* from stock_info a left join stock_daily b on a.code = b.code "
                           "where a.code = 'sh601179' order by b.trade_date desc limit 60",
                           DataFrame(meta={Decimal: float}))
        df = df.sort_values(by='trade_date', ascending=True)

        # trade_date = [d.strftime('%Y/%m/%d')[2:] for d in df['trade_date']]
        # ma = talib.MA(df['close'], timeperiod=5)
        # ma = [round(v, 3) for v in ma]
        # line1 = plot_chart(chart_cls=Line, x_index=trade_date, y_data=ma,
        #                    is_smooth=True, title='line2')
        #
        # max_val, min_val = df['high'].max(), df['low'].min()
        # max_y, min_y = int(max_val * 1.1), int(min_val * 0.9)
        # line1.set_global_opts(yaxis_opts=opts.AxisOpts(max_=max_y,
        #                                                min_=min_y, ))
        #
        # line = plot_grid(df, '测试', line1)
        line = my_plot(df, marks=[
            {'color': up_color(), 'data': [{'trade_date': datetime(year=2021, month=9, day=1), 'tip': '测试\n测试'},
                                           {'trade_date': datetime(year=2021, month=8, day=26), 'tip': '测试2\n测试2'}]}])
        line.render('/Users/luoguochun/Downloads/a.html')


    run_until_complete(test())
