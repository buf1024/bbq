import numpy as np
import pandas as pd
import scipy.signal as signal
from sklearn.linear_model import LinearRegression
from typing import Tuple, List


def extrema(data: pd.DataFrame, *, field='close', cmp_func=np.greater, index=None) -> Tuple[List, List, List]:
    """
    求极值点
    :param index: 实际index
    :param data: DataFrame 或 需要拟合的点
    :param field: DataFrame 时, 拟合点域
    :param cmp_func: 极值点, np.greater / np.less
    :return: （x坐标, x极值下标, y极值点)
    """
    x_data = np.array(data[field])
    if index is None:
        index = [dt.strftime('%Y/%m/%d')[2:] for dt in data['trade_date']]

    x_data_ext = signal.argrelextrema(x_data, cmp_func)
    y_data_ext = x_data[x_data_ext[0]]

    x_index = np.array(index)[x_data_ext[0]]

    return list(x_index), list(x_data_ext[0]), list(y_data_ext)


def linear_fitting(data: pd.DataFrame, *, field='close') -> \
        Tuple[float, float, float, List, List]:
    """
    线性拟合股价
    :param data: DataFrame 或 需要拟合的点
    :param field: DataFrame 时, 拟合点域
    :return: （斜率, 截距, 拟合度/评分, x坐标, y拟合点, y极值点)
    """
    index = [dt.strftime('%Y/%m/%d')[2:] for dt in data['trade_date']]
    _, x_max_data_ext, y_max_data_ext = extrema(data=data, field=field, cmp_func=np.greater, index=index)
    _, x_min_data_ext, y_min_data_ext = extrema(data=data, field=field, cmp_func=np.less, index=index)

    x_data_ext = x_max_data_ext + x_min_data_ext
    y_data_ext = y_max_data_ext + y_min_data_ext

    # 转换成numpy的ndarray数据格式，n行1列,LinearRegression需要列格式数据，如下：
    x_train = np.array(x_data_ext).reshape((len(x_data_ext), 1))
    y_train = np.array(y_data_ext).reshape((len(y_data_ext), 1))

    if len(x_train) == 0 or len(y_train) == 0:
        return None, None, None, None, None

    # 新建一个线性回归模型，并把数据放进去对模型进行训练
    line_model = LinearRegression()
    line_model.fit(x_train, y_train)

    # coef_是系数，intercept_是截距，拟合的直线是y=ax+b
    a = line_model.coef_[0][0]
    b = line_model.intercept_[0]

    # 对回归模型进行评分，这里简单使用训练集进行评分，实际很多时候用其他的测试集进行评分
    score = line_model.score(x_train, y_train)

    # 用训练后的模型，进行预测
    x_predict = np.array(range(data.shape[0])).reshape((data.shape[0], 1))
    x_index = np.array(index)[list(range(data.shape[0]))]
    y_predict = line_model.predict(x_predict)

    x_index = list(x_index)
    y_index = [x for v in y_predict for x in v]

    return a, b, score, x_index, y_index
