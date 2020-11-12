from common import load_strategy
from os.path import dirname

__file_path = dirname(__file__)

strategies = load_strategy(__file_path, 'selector.strategy', ('strategy.py',))
