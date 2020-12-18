import talib
import pandas as pd

if __name__ == '__main__':
    df = pd.DataFrame({'open': [100], 'high': [100], 'low': [20], 'close': [80]})
    print(df)
    print(talib.CDLHAMMER(df['open'], df['high'], df['low'], df['close']))
