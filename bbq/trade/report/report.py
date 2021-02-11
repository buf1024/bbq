from ..account import Account
import pandas as pd
from pyecharts import options as opts
from pyecharts.charts import *
from pyecharts.charts.chart import Chart
from pyecharts.globals import SymbolType
from bbq.analyse.plot import \
    kline_tooltip_fmt_func, kline_orig_data, up_color, down_color, plot_overlap
from typing import Optional, Sequence
from datetime import datetime


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

    pos_top = 50
    empty_height = 100
    line_height = 100
    kline_height = 250

    def __init__(self, account: Account):
        self.account = account
        self.db_data = account.db_data

        self.is_backtest = account.trader.is_backtest()
        self.acct_his = None
        self.deal_his = None
        self.trade_date = []

        self.is_ready = False

    async def collect_data(self):
        is_end = self.account.start_time is not None and self.account.end_time is not None
        if not is_end:
            self.is_ready = False
            return False

        start_time = datetime(year=self.account.start_time.year, month=self.account.start_time.month,
                              day=self.account.start_time.day)
        end_time = datetime(year=self.account.end_time.year, month=self.account.end_time.month,
                            day=self.account.end_time.day)

        self.trade_date = list(pd.date_range(start_time, end_time))

        self.acct_his = self.account.acct_his
        if not self.is_backtest:
            self.acct_his = await self.db_data.load_account_his(filter={'account_id': self.account.account_id},
                                                                sort=[('end_time', 1)])

        acct_his_df = pd.DataFrame(self.acct_his)
        if not acct_his_df.empty:
            acct_his_df['start_time'] = acct_his_df['start_time'].apply(lambda d:
                                                                        datetime(year=d.start_time.year,
                                                                                 month=d.start_time.month,
                                                                                 day=d.start_time.day))
            acct_his_df['end_time'] = acct_his_df['end_time'].apply(lambda d:
                                                                    datetime(year=d.end_time.year,
                                                                             month=d.end_time.month,
                                                                             day=d.end_time.day))
            acct_his_df.index = acct_his_df['end_time']

            acct_his_all_df = pd.DataFrame(index=self.trade_date, data=[])
            acct_his_all_df = acct_his_all_df.merge(acct_his_df, how='left', left_index=True, right_index=True)
            acct_his_all_df.fillna(method='ffill', inplace=True)

            self.acct_his = acct_his_all_df

        if not self.is_backtest:
            self.deal_his = await self.db_data.load_deal(filter={'account_id': self.account.account_id})
        else:
            self.deal_his = []
            for deal in self.account.deal:
                self.deal_his.append(deal.to_dict())
        deal_his_df = pd.DataFrame(self.deal_his)
        if not deal_his_df.empty:
            self.deal_his = deal_his_df

        return True

    def _plot_cash(self, grid, pos, height):
        def _text(data):
            up = round((data[-1] - data[0]) / data[0] * 100, 2)
            return f'{data[0]}, {data[-1]}, {up}'

        x_data = self.trade_date

        graphic_text = []

        # line cash
        y_data = list(self.acct_his['cash_available'])
        line_cash = Line()
        line_cash.add_xaxis(xaxis_data=x_data)
        line_cash.add_yaxis(
            series_name='资金', y_axis=y_data,
            label_opts=opts.LabelOpts(is_show=False),
            is_smooth=False, symbol='none',
            itemstyle_opts=opts.ItemStyleOpts(color=self.color_down))

        graphic_text.append(
            opts.GraphicText(
                graphic_item=opts.GraphicItem(left='left', top=f'{pos}px', z=-100, ),
                graphic_textstyle_opts=opts.GraphicTextStyleOpts(
                    text=f'  -- 资金({_text(y_data)})', font=self.color_down,
                    graphic_basicstyle_opts=opts.GraphicBasicStyleOpts(fill=self.color_down)
                )
            )
        )

        # line net
        y_data = list(self.acct_his['total_net_value'])
        max_y, min_y = max(y_data), min(y_data)
        max_rate = round((max_y - self.account.cash_init) / self.account.cash_init * 100, 2),
        min_rate = round((min_y - self.account.cash_init) / self.account.cash_init * 100, 2)
        line_net = Line()
        line_net.add_xaxis(xaxis_data=x_data)
        line_net.add_yaxis(
            series_name='净值', y_axis=y_data,
            label_opts=opts.LabelOpts(is_show=False),
            is_smooth=False, symbol='none',
            markline_opts=opts.MarkLineOpts(
                symbol='none', data=[
                    opts.MarkLineItem(name=f'MAX({max_y}, {max_rate}%)', y=max_y, symbol='none'),
                    opts.MarkLineItem(name=f'MIN({min_y}, {min_rate}%)', y=min_y, symbol='none'),
                ],
                label_opts=opts.LabelOpts(position='insideMiddle')
            ),
            itemstyle_opts=opts.ItemStyleOpts(color=self.color_up)
        )
        graphic_text.append(
            opts.GraphicText(
                graphic_item=opts.GraphicItem(left='left', top=f'{pos + 20}px', z=-100, ),
                graphic_textstyle_opts=opts.GraphicTextStyleOpts(
                    text=f'  -- 净值({y_data[-1]})', font=self.color_up,
                    graphic_basicstyle_opts=opts.GraphicBasicStyleOpts(fill=self.color_up)
                )
            )
        )

        line = plot_overlap(line_cash, line_net)
        line.set_global_opts(
            xaxis_opts=opts.AxisOpts(is_show=False, is_scale=True),
            yaxis_opts=opts.AxisOpts(
                position='left', is_scale=True,
                splitarea_opts=opts.SplitAreaOpts(
                    is_show=True, areastyle_opts=opts.AreaStyleOpts(opacity=1)
                ),
            ),
            legend_opts=opts.LegendOpts(pos_left='10%', orient='vertical'),
            graphic_opts=opts.GraphicGroup(
                graphic_item=opts.GraphicItem(left='10%', top=pos, ),
                children=graphic_text,
            )
        )

        grid.add(line, grid_opts=opts.GridOpts(pos_top=f'{pos}px', height=f'{height}px'))
        return grid

    def _plot_profit(self, grid, pos, height) -> Optional[Chart]:
        graphic_text = []
        x_data = []
        y_data = []

        df = self.deal_his[self.deal_his['profit'] > 0] if not self.deal_his.empty else pd.DataFrame()
        text = f'  -- 盈利(0元, 0%, 0次, 0%)'
        if not df.empty:
            x_data = list(df['time'].apply(lambda d: d.strftime('%Y/%m/%d')[2:]))
            y_data = list(df['profit'])
            profit_total, times = round(sum(df['profit']), 2), df.shape[0]
            win_rate, rate = round(df.shape[0] / self.deal_his.shape[0] * 100, 2), round(
                sum(df['profit']) / self.account.cash_init * 100, 2)
            text = f'  -- 盈利({profit_total}元, {rate}%, {times}次, {win_rate}%)'

        graphic_text.append(
            opts.GraphicText(
                graphic_item=opts.GraphicItem(left='left', top=f'{pos + 20}px', z=-100, ),
                graphic_textstyle_opts=opts.GraphicTextStyleOpts(
                    text=text, font=self.color_up,
                    graphic_basicstyle_opts=opts.GraphicBasicStyleOpts(fill=self.color_up)
                )
            )
        )

        scatter_up = Scatter()
        scatter_up.add_xaxis(x_data)
        scatter_up.add_yaxis('盈利', y_data, symbol_size=15,
                             itemstyle_opts=opts.ItemStyleOpts(color=self.color_up))

        df = self.deal_his[self.deal_his['profit'] < 0]
        text = f'  -- 亏损(0元, 0%, 0次, 0%)'
        if not df.empty:
            x_data = list(df['time'].apply(lambda d: d.strftime('%Y/%m/%d')[2:]))
            y_data = list(df['profit'])
            profit_total, times = round(sum(df['profit']), 2), df.shape[0]
            lost_rate, rate = round(df.shape[0] / self.deal_his.shape[0] * 100, 2), round(
                sum(df['profit']) / self.account.cash_init * 100, 2)
            text = f'  -- 盈利({profit_total}元, {rate}%, {times}次, {lost_rate}%)'

        scatter_down = Scatter()
        scatter_down.add_xaxis(x_data)
        scatter_down.add_yaxis('亏损', y_data, symbol_size=15,
                               itemstyle_opts=opts.ItemStyleOpts(color=self.color_down))

        graphic_text.append(
            opts.GraphicText(
                graphic_item=opts.GraphicItem(left='left', top=f'{pos + 20}px', z=-100, ),
                graphic_textstyle_opts=opts.GraphicTextStyleOpts(
                    text=text, font=self.color_down,
                    graphic_basicstyle_opts=opts.GraphicBasicStyleOpts(fill=self.color_down)
                )
            )
        )

        scatter = plot_overlap(scatter_up, scatter_down)
        scatter.set_global_opts(
            xaxis_opts=opts.AxisOpts(is_show=False, is_scale=True),
            yaxis_opts=opts.AxisOpts(
                position='right', is_scale=True,
                splitarea_opts=opts.SplitAreaOpts(
                    is_show=True, areastyle_opts=opts.AreaStyleOpts(opacity=1)
                ),
            ),
            legend_opts=opts.LegendOpts(pos_left='20%', orient='vertical'),
            graphic_opts=opts.GraphicGroup(
                graphic_item=opts.GraphicItem(left='10%', top=pos, ),
                children=graphic_text,
            )
        )

        grid.add(scatter, grid_opts=opts.GridOpts(pos_top=f'{pos}px', height=f'{height}px'))
        return grid

    def _plot_kline(self) -> Optional[Sequence[Chart]]:
        pass

    def plot(self):
        if not self.is_ready:
            return None

        # todo
        height = self.pos_top + self.line_height * 2 + self.kline_height + self.empty_height
        grid = Grid(init_opts=opts.InitOpts(height=f'{height}px'))

        pos = self.pos_top
        self._plot_cash(grid=grid, pos=pos, height=self.line_height)
        profit_scatter = self._plot_profit()
        klines = self._plot_kline()

        # grid.add(cash_line, grid_opts=opts.GridOpts())

        pos = pos + self.line_height
        grid.add(profit_scatter, grid_opts=opts.GridOpts(pos_top=pos, height=self.line_height))

        pos = pos + self.line_height
        for kline in klines:
            grid.add(kline, grid_opts=opts.GridOpts(pos_top=pos, height=self.kline_height))
            pos = pos + self.kline_height

        return grid
