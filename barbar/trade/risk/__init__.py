from common import load_strategy
from os.path import sep, dirname

__file_path = dirname(__file__)

risks= load_strategy(__file_path, 'trader.risk', ('risk.py', ))
