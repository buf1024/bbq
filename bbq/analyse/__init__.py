from .plot import *
from .tools import linear_fitting, extrema

import pyecharts.options as pyc_opt
import pyecharts.charts as pyc_ch

import talib as ta
import talib.abstract as ta_abs

__all__ = ['my_plot', 'plot_volume', 'plot_kline', 'plot_chart', 'plot_overlap', 'plot_grid', 'JsCode',
           'linear_fitting', 'extrema']

for mod, names in {ta_abs: list(ta_abs.__dict__.keys()),
                   ta: list(ta.__dict__.keys()),
                   pyc_opt: list(pyc_opt.__dict__.keys()),
                   pyc_ch: list(pyc_ch.__dict__.keys()),
                   }.items():
    names = [name for name in names if not name.startswith('_')]
    globals().update({name: getattr(mod, name) for name in names})
    __all__.extend(names)
