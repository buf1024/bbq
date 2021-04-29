# from ..account import Account
from datetime import datetime
from typing import Optional, Sequence
import pandas as pd
import bbq.fetch as fetch
from pyecharts import options as opts
from pyecharts.charts import *
from pyecharts.charts.chart import Chart
from bbq.analyse.plot import \
    up_color, down_color, plot_overlap, plot_kline
from bbq.data import FundDB
from pyecharts.components import Table
from pyecharts.options import ComponentTitleOpts


#
# def _plot_kline(data: pd.DataFrame, *, title: str = '日线',
#                 overlap: Sequence = ('MA5', 'MA10', 'MA20')):
#     kdata = list(zip(data['open'], data['close'], data['low'], data['high']))
#     trade_date = [d.strftime('%Y/%m/%d')[2:] for d in data['trade_date']]
#
#     kline = Kline()
#     kline.add_js_funcs('var kdata={}'.format(kline_orig_data(data)))
#     kline.add_js_funcs('var trade_date={}'.format(trade_date))
#     kline.add_js_funcs('var colors=["{}", "{}"]'.format(up_color(), down_color()))
#     kline.add_xaxis(trade_date)
#     kline.add_yaxis(series_name=title, y_axis=kdata,
#                     itemstyle_opts=opts.ItemStyleOpts(
#                         color=up_color(),
#                         color0=down_color(),
#                     ),
#                     tooltip_opts=opts.TooltipOpts(
#                         formatter=kline_tooltip_fmt_func())
#                     )
#

class Report:
    color_up = up_color()
    color_down = down_color()

    arrow_up = 'path://M752.64 376.32 546.56 202.88c-19.2-16-50.56-16-69.76 0L270.72 376.32C240 402.56 261.76 447.36 ' \
               '305.28 447.36L384 447.36C384 447.36 384 448 384 448l0 320c0 35.2 28.8 64 64 64l128 0c35.2 0 64-28.8 ' \
               '64-64L640 448c0 0 0-0.64 0-0.64l78.08 0C761.6 447.36 783.36 402.56 752.64 376.32z'
    arrow_down = 'path://M719.36 575.36l-77.44 0c0-0.64 0-0.64 0-1.28L641.92 256c0-35.2-28.8-64-64-64L448 192C412.8 ' \
                 '192 384 220.8 384 256l0 318.08c0 0.64 0 0.64 0 1.28L305.92 575.36c-44.16 0-65.92 44.8-35.2 ' \
                 '70.4l206.72 173.44c19.2 16 50.56 16 69.76 0l206.72-173.44C785.28 620.16 763.52 575.36 719.36 ' \
                 '575.36z'

    pos_top = 75

    def __init__(self, account):
        self.account = account
        self.db_data = account.db_data
        self.db_trade = account.db_trade

        self.is_backtest = account.trader.is_backtest()
        self.acct_his = None
        self.deal_his = None
        self.trade_date = []

        self.codes = []
        self.daily = None

        self.is_ready = False

    @staticmethod
    def _convert_time(d):
        if isinstance(d, str) and '/' in d:
            d = datetime.strptime(d[:len('2020-01-01')], '%Y/%m/%d')
        if isinstance(d, str) and '-' in d:
            d = datetime.strptime(d[:len('2020-01-01')], '%Y-%m-%d')
        return datetime(year=d.year, month=d.month, day=d.day)

    @staticmethod
    def _to_x_data(lst, target='str'):
        data = []
        is_list = True
        if not isinstance(lst, list):
            lst = [lst]
            is_list = False
        for trade_date in lst:
            if fetch.is_trade_date(trade_date):
                trade_date = Report._convert_time(trade_date)
                if target == 'str':
                    data.append(trade_date.strftime('%Y/%m/%d')[2:])
                else:
                    data.append(trade_date)
        if not is_list:
            if len(data) > 0:
                data = data[0]
        return data

    async def collect_data(self):
        is_end = self.account.start_time is not None and self.account.end_time is not None
        if not is_end:
            self.is_ready = False
            return False

        start_time = datetime(year=self.account.start_time.year, month=self.account.start_time.month,
                              day=self.account.start_time.day)
        end_time = datetime(year=self.account.end_time.year, month=self.account.end_time.month,
                            day=self.account.end_time.day)

        self.trade_date = self._to_x_data(list(pd.date_range(start_time, end_time)), 'datetime')

        self.acct_his = self.account.acct_his
        if not self.is_backtest:
            self.acct_his = await self.db_trade.load_account_his(filter={'account_id': self.account.account_id},
                                                                 sort=[('end_time', 1)])

        acct_his_df = pd.DataFrame(self.acct_his)
        if not acct_his_df.empty:
            acct_his_df['start_time'] = acct_his_df['start_time'].apply(self._convert_time)
            acct_his_df['end_time'] = acct_his_df['end_time'].apply(self._convert_time)
            acct_his_df.index = acct_his_df['end_time']

            acct_his_all_df = pd.DataFrame(index=self.trade_date, data=[])
            acct_his_all_df = acct_his_all_df.merge(acct_his_df, how='left', left_index=True, right_index=True)
            acct_his_all_df.fillna(method='ffill', inplace=True)

            self.acct_his = acct_his_all_df

        deal_his = self.account.deal
        if not self.is_backtest:
            deal_his = await self.db_trade.load_deal(filter={'account_id': self.account.account_id},
                                                     sort=[('time', 1)])
        deal_his_df = pd.DataFrame(deal_his)
        if not deal_his_df.empty:
            # deal_his_df = deal_his_df[deal_his_df['type'] == 'sell']
            deal_his_df['trade_date'] = deal_his_df['time'].apply(self._convert_time)
            self.deal_his = deal_his_df

            deal_his_group = deal_his_df.groupby('code')
            self.codes = list(deal_his_group.groups.keys())
            func_daily = self.db_data.load_fund_daily if isinstance(self.db_data,
                                                                    FundDB) else self.db_data.load_stock_daily
            self.daily = await func_daily(filter={'code': {'$in': self.codes},
                                                  'trade_date': {'$gte': start_time, '$lte': end_time}},
                                          sort=[('trade_date', 1)])

        self.trade_date = self._to_x_data(self.trade_date)

        self.acct_his.to_csv('/Users/luoguochun/Downloads/acct_his.csv')
        self.deal_his.to_csv('/Users/luoguochun/Downloads/deal_his.csv')
        self.daily.to_csv('/Users/luoguochun/Downloads/daily.csv')
        self.is_ready = True
        return self.is_ready

    def plot_cash(self):
        def _text(data):
            if len(data) == 0:
                return f'0元, 0元, 0%'
            up = round((data[-1] - data[0]) / data[0] * 100, 2)
            return f'{data[0]}元, {data[-1]}元, {up}%'

        x_data = self.trade_date

        graphic_text = []

        # line cash
        y_data = list(self.acct_his['cash_available'])
        line_cash = Line()
        line_cash.add_xaxis(xaxis_data=x_data)
        line_cash.add_yaxis(
            series_name='资金', y_axis=y_data,
            label_opts=opts.LabelOpts(is_show=False),
            is_smooth=True, symbol='none',
        )

        graphic_text.append(
            opts.GraphicText(
                graphic_item=opts.GraphicItem(left='left', top=f'{self.pos_top}px', z=-100, ),
                graphic_textstyle_opts=opts.GraphicTextStyleOpts(
                    text=f'  -- 资金({_text(y_data)})',
                    font=self.color_down,
                    graphic_basicstyle_opts=opts.GraphicBasicStyleOpts(fill=self.color_up)
                )
            )
        )

        # line net
        y_data = list(self.acct_his['total_net_value'])
        max_y, min_y = max(y_data), min(y_data)
        max_rate = round((max_y - self.account.cash_init) / self.account.cash_init * 100, 2),
        min_rate = round((min_y - self.account.cash_init) / self.account.cash_init * 100, 2)
        max_back = round((max_y - min_y) / max_y * 100, 2)
        max_x_coord = self.trade_date[y_data.index(max_y)]
        min_x_coord = self.trade_date[y_data.index(min_y)]
        line_net = Line()
        line_net.add_xaxis(xaxis_data=x_data)
        line_net.add_yaxis(
            series_name='净值', y_axis=y_data,
            label_opts=opts.LabelOpts(is_show=False),
            is_smooth=True, symbol='none',
            markpoint_opts=opts.MarkPointOpts(
                data=[
                    opts.MarkPointItem(
                        name=f'MAX({max_x_coord}, {max_y})',
                        symbol=self.arrow_down, symbol_size=10, coord=[max_x_coord, max_y * 1.0005],
                        itemstyle_opts=opts.ItemStyleOpts(color=self.color_up)
                    ),
                    opts.MarkPointItem(
                        name=f'MIN({min_x_coord}, {min_y})',
                        symbol=self.arrow_up, symbol_size=10, coord=[min_x_coord, min_y * 0.9995],
                        itemstyle_opts=opts.ItemStyleOpts(color=self.color_up)
                    ),
                ]
            ),
        )
        graphic_text.append(
            opts.GraphicText(
                graphic_item=opts.GraphicItem(left='left', top=f'{self.pos_top + 20}px', z=-100, ),
                graphic_textstyle_opts=opts.GraphicTextStyleOpts(
                    text=f'  -- 净值({_text(y_data)})',
                    font=self.color_up,
                    graphic_basicstyle_opts=opts.GraphicBasicStyleOpts(fill=self.color_up)
                )
            )
        )
        graphic_text.append(
            opts.GraphicText(
                graphic_item=opts.GraphicItem(left='left', top=f'{self.pos_top + 40}px', z=-100, ),
                graphic_textstyle_opts=opts.GraphicTextStyleOpts(
                    text=f'  -- 最大回撤: {max_back}%',
                    font=self.color_up,
                    graphic_basicstyle_opts=opts.GraphicBasicStyleOpts(fill=self.color_up)
                )
            )
        )
        graphic_text.append(
            opts.GraphicText(
                graphic_item=opts.GraphicItem(left='left', top=f'{self.pos_top + 60}px', z=-100, ),
                graphic_textstyle_opts=opts.GraphicTextStyleOpts(
                    text=f'  -- 最大盈亏: {round(max_y - self.account.cash_init, 4)}({max_rate}%)',
                    font=self.color_up,
                    graphic_basicstyle_opts=opts.GraphicBasicStyleOpts(fill=self.color_up)
                )
            )
        )
        graphic_text.append(
            opts.GraphicText(
                graphic_item=opts.GraphicItem(left='left', top=f'{self.pos_top + 80}px', z=-100, ),
                graphic_textstyle_opts=opts.GraphicTextStyleOpts(
                    text=f'  -- 最小盈亏: {round(min_y - self.account.cash_init, 4)}({min_rate}%)',
                    font=self.color_up,
                    graphic_basicstyle_opts=opts.GraphicBasicStyleOpts(fill=self.color_up)
                )
            )
        )

        line = plot_overlap(line_cash, line_net)
        line.set_global_opts(
            xaxis_opts=opts.AxisOpts(is_show=True, is_scale=True),
            yaxis_opts=opts.AxisOpts(
                position='left', is_scale=True,
                splitarea_opts=opts.SplitAreaOpts(
                    is_show=True, areastyle_opts=opts.AreaStyleOpts(opacity=1)
                ),
            ),
            graphic_opts=opts.GraphicGroup(
                graphic_item=opts.GraphicItem(left='10%'),
                children=graphic_text,
            ),
            datazoom_opts=[opts.DataZoomOpts(pos_bottom="-2%", filter_mode='none')],
            title_opts=opts.TitleOpts(title='资金变动')
        )

        return line

    def plot_profit(self) -> Optional[Chart]:
        line = Line()
        line.add_xaxis(xaxis_data=self.trade_date)
        line.add_yaxis(series_name='', y_axis=[])

        deal_his = self.deal_his[self.deal_his['type'] == 'sell'] if not self.deal_his.empty else pd.DataFrame()
        deal_his = deal_his[['trade_date', 'profit']] if not deal_his.empty else pd.DataFrame()
        win_text = f'  -- 盈利(0元, 0%, 0次, 0%)'
        lost_text = f'  -- 亏损(0元, 0%, 0次, 0%)'
        if not deal_his.empty:
            times = deal_his.shape[0]

            win_df = deal_his[deal_his['profit'] >= 0]
            win_times = win_df.shape[0]
            win_times_rate = round(win_times / times * 100, 2)
            win_rate = round(sum(win_df['profit']) / self.account.cash_init * 100, 2) if not win_df.empty else 0
            win_total = round(sum(win_df['profit']), 2) if not win_df.empty else 0
            win_text = f'  -- 盈利({win_total}元, {win_rate}%, {win_times}次, {win_times_rate}%)'

            lost_df = deal_his[deal_his['profit'] < 0]
            lost_times = lost_df.shape[0]
            lost_times_rate = round(lost_times / times * 100, 2)
            lost_rate = round(sum(lost_df['profit']) / self.account.cash_init * 100, 2) if not lost_df.empty else 0
            lost_total = round(sum(lost_df['profit']), 2) if not lost_df.empty else 0
            lost_text = f'  -- 亏损({lost_total}元, {lost_rate}%, {lost_times}次, {lost_times_rate}%)'

            deal_his_group = deal_his.groupby('trade_date').sum()
            deal_his_group.reset_index(inplace=True)
            df = deal_his_group[deal_his_group['profit'] >= 0]

            if not df.empty:
                x_data = self._to_x_data(list(df['trade_date']))
                y_data = [round(x, 2) for x in list(df['profit'])]
                scatter_up = Scatter()
                scatter_up.add_xaxis(x_data)
                scatter_up.add_yaxis('盈利', y_data, itemstyle_opts=opts.ItemStyleOpts(color=self.color_up))
                line = plot_overlap(line, scatter_up)

            df = deal_his_group[deal_his_group['profit'] < 0]
            if not df.empty:
                x_data = self._to_x_data(list(df['trade_date']))
                y_data = [round(x, 2) for x in list(df['profit'])]
                scatter_down = Scatter()
                scatter_down.add_xaxis(x_data)
                scatter_down.add_yaxis('亏损', y_data, itemstyle_opts=opts.ItemStyleOpts(color=self.color_down))
                line = plot_overlap(line, scatter_down)

        line.set_global_opts(
            xaxis_opts=opts.AxisOpts(is_show=True, is_scale=True),
            yaxis_opts=opts.AxisOpts(
                position='left', is_scale=True,
                splitarea_opts=opts.SplitAreaOpts(
                    is_show=True, areastyle_opts=opts.AreaStyleOpts(opacity=1)
                ),
            ),
            graphic_opts=opts.GraphicGroup(
                graphic_item=opts.GraphicItem(left='10%'),
                children=[
                    opts.GraphicText(
                        graphic_item=opts.GraphicItem(left='left', top=f'{self.pos_top}px', z=-100, ),
                        graphic_textstyle_opts=opts.GraphicTextStyleOpts(
                            text=win_text, font=self.color_up,
                            graphic_basicstyle_opts=opts.GraphicBasicStyleOpts(fill=self.color_up)
                        )
                    ),
                    opts.GraphicText(
                        graphic_item=opts.GraphicItem(left='left', top=f'{self.pos_top + 20}px', z=-100, ),
                        graphic_textstyle_opts=opts.GraphicTextStyleOpts(
                            text=lost_text, font=self.color_down,
                            graphic_basicstyle_opts=opts.GraphicBasicStyleOpts(fill=self.color_down)
                        )
                    )
                ],
            ),
            datazoom_opts=[opts.DataZoomOpts(pos_bottom="-2%", filter_mode='none')],
            title_opts=opts.TitleOpts(title='盈亏')
        )

        return line

    def _plot_kline(self, code, code_sell, code_buy) -> Optional[Chart]:
        def get_markpoints(color, typ, symbol, factor, data):
            mark_points = {}
            for item in data.to_dict('records'):
                index = self._to_x_data(item['trade_date'])
                tm = item['time']
                if isinstance(tm, str):
                    if '/' in tm:
                        tm = datetime.strptime(tm, '%Y/%m/%d %H:%M:%S')
                    else:
                        tm = datetime.strptime(tm, '%Y-%m-%d %H:%M:%S')
                t = tm.strftime('%H:%M')
                index = self.trade_date.index(index)
                close = daily_close[index]
                if index not in mark_points:
                    mark_points[index] = dict(name=f'{typ}:{item["price"]},{item["volume"]},{t}',
                                              symbol=symbol,
                                              color=color,
                                              coord=[index, close * factor])
                else:
                    mark_points[index]['name'] = mark_points[index]['name'] + f'{item["price"]},{item["volume"]},{t}'
            return mark_points.values()

        daily = self.daily[self.daily['code'] == code]
        daily_close = list(daily['close'])
        line = Line()
        line.add_xaxis(xaxis_data=self.trade_date)
        points = []
        if not code_sell.empty:
            p = get_markpoints(color=self.color_down, typ='S', symbol=self.arrow_down, factor=1.002, data=code_sell)
            points = points + list(p)
        if not code_buy.empty:
            p = get_markpoints(color=self.color_up, typ='B', symbol=self.arrow_up, factor=0.998, data=code_buy)
            points = points + list(p)

        data = []
        for point in points:
            opt = opts.MarkPointItem(name=point['name'], symbol=point['symbol'],
                                     symbol_size=10, coord=point['coord'],
                                     itemstyle_opts=opts.ItemStyleOpts(color=point['color']))
            data.append(opt)

        line.add_yaxis(series_name='收盘价',
                       label_opts=opts.LabelOpts(is_show=False),
                       is_smooth=True, symbol='none',
                       y_axis=daily_close, markpoint_opts=opts.MarkPointOpts(data=data))
        kline = plot_kline(data=daily, overlap=('MA5', 'MA10', 'MA20', line))
        return kline

    def plot_kline(self) -> Optional[Sequence[Chart]]:
        charts = []
        if len(self.codes) > 0 and self.daily is not None:
            deal_his = self.deal_his if not self.deal_his.empty else pd.DataFrame()
            deal_his_sell = deal_his[deal_his['type'] == 'sell']
            deal_his_buy = deal_his[deal_his['type'] == 'buy']
            for code in self.codes:
                code_sell = deal_his_sell[deal_his_sell['code'] == code] if not deal_his_sell.empty else pd.DataFrame()
                code_buy = deal_his_buy[deal_his_buy['code'] == code] if not deal_his_buy.empty else pd.DataFrame()
                chart = self._plot_kline(code, code_sell, code_buy)
                charts.append(chart)
        return charts

    def plot_static(self):
        table = Table()

        headers = ["策略", " ", "  ", "   "]
        rows = [
            ['账户', '', '', ''],
            ['期初', '', '期末', '', ],
            ["Darwin", 112, 120900, 1714.7],
            ["Hobart", 1357, 205556, 619.5],
            ["Sydney", 2058, 4336374, 1214.8],
            ["Melbourne", 1566, 3806092, 646.9],
            ["Perth", 5386, 1554769, 869.4],
        ]
        table.add(headers, rows)
        table.set_global_opts(
            title_opts=ComponentTitleOpts(title="统计")
        )
        return table

    def plot(self):
        if not self.is_ready:
            return None

        page = Page(page_title='策略统计')
        page.add(self.plot_cash())
        page.add(self.plot_profit())
        klines = self.plot_kline()
        for kline in klines:
            page.add(kline)
        # page.add(self.plot_static())
        return page


if __name__ == '__main__':
    class Account:
        cash_init = 100000
        color_up = '#F11300'
        color_down = '#00A800'

        class Trader:
            @staticmethod
            def is_backtest():
                return True

        trader = Trader()
        db_data = None
        db_trade = None


    report = Report(Account())
    report.acct_his = pd.read_csv('/Users/luoguochun/Downloads/acct_his.csv', index_col=0)
    report.acct_his['start_time'] = pd.to_datetime(report.acct_his['start_time'])
    report.acct_his['end_time'] = pd.to_datetime(report.acct_his['end_time'])

    report.deal_his = pd.read_csv('/Users/luoguochun/Downloads/deal_his.csv', index_col=0)
    report.deal_his['time'] = pd.to_datetime(report.deal_his['time'])
    report.deal_his['trade_date'] = pd.to_datetime(report.deal_his['trade_date'])

    report.daily = pd.read_csv('/Users/luoguochun/Downloads/daily.csv', index_col=0)
    report.daily['trade_date'] = pd.to_datetime(report.daily['trade_date'])

    report.is_ready = True
    report.trade_date = []
    report.codes = ['sh601618']

    for trade_date in list(pd.date_range('2020-12-01', '2020-12-31')):
        if fetch.is_trade_date(trade_date):
            report.trade_date.append(trade_date.strftime('%Y/%m/%d')[2:])

    # line = report.plot_cash()
    # line = report.plot_profit()
    # line = report.plot_kline()
    line = report.plot()
    line.render('/Users/luoguochun/Downloads/render.html')
