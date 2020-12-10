from bbq.trade.base_obj import BaseObj
import sys


class Position(BaseObj):
    def __init__(self, position_id: str, account):
        super().__init__(typ=account.typ, db_data=account.db_data, db_trade=account.db_trade, trader=account.trader)
        self.account = account

        self.position_id = position_id

        self.name = ''  # 股票名称
        self.code = ''  # 股票代码
        self.time = None  # 首次建仓时间

        self.volume = 0  # 持仓量
        self.volume_available = 0  # 可用持仓量
        self.volume_frozen = 0  # 可用持仓量

        self.fee = 0.0  # 持仓费用
        self.price = 0.0  # 平均持仓价

        self.now_price = 0.0  # 最新价
        self.max_price = 0.0  # 最高价
        self.min_price = 0.0  # 最低价

        self.profit_rate = 0.0  # 盈利比例
        self.max_profit_rate = -sys.maxsize  # 最大盈利比例
        self.min_profit_rate = sys.maxsize  # 最小盈利比例

        self.profit = 0.0  # 盈利
        self.max_profit = -sys.maxsize  # 最大盈利
        self.min_profit = sys.maxsize  # 最小盈利

        self.max_profit_time = None  # 最大盈利时间
        self.min_profit_time = None  # 最小盈利时间

    async def on_quot(self, payload):
        self.now_price = payload['close']
        self.max_price = self.now_price if self.max_price < self.now_price else self.max_price
        self.min_price = self.now_price if self.min_price > self.now_price else self.min_price

        self.profit = (self.now_price - self.price) * self.volume - self.fee
        self.profit_rate = self.profit / (self.price * self.volume + self.fee)
        if self.profit > self.max_profit:
            self.max_profit = self.profit
            self.max_profit_time = payload['day_time']
            self.max_profit_rate = self.profit_rate

        if self.profit < self.max_profit:
            self.min_profit = self.profit
            self.min_profit_time = payload['day_time']
            self.min_profit_rate = self.profit_rate

        await self.sync_to_db()

    async def sync_from_db(self) -> bool:
        position = await self.db_trade.load_position(filter={'position_id': self.position_id}, limit=1)
        position = None if len(position) == 0 else position[0]
        if position is None:
            self.log.error('position from db not found: {}'.format(self.position_id))
            return False
        self.name = position['name']
        self.code = position['code']
        self.volume = position['volume']
        self.volume_available = position['volume_available']
        self.fee = position['fee']
        self.price = position['price']
        self.profit_rate = position['profit_rate']
        self.max_profit_rate = position['max_profit_rate']
        self.min_profit_rate = position['min_profit_rate']
        self.profit = position['profit']
        self.max_profit = position['max_profit']
        self.min_profit = position['min_profit']
        self.now_price = position['now_price']
        self.max_price = position['max_price']
        self.min_price = position['min_price']
        self.max_profit_time = position['max_profit_time']
        self.min_profit_time = position['min_profit_time']
        self.time = position['time']
        return True

    @BaseObj.discard_saver
    async def sync_to_db(self) -> bool:
        data = {'account_id': self.account.account_id,
                'position_id': self.position_id, 'name': self.name, 'code': self.code,
                'volume': self.volume, 'volume_available': self.volume_available,
                'fee': self.fee, 'price': self.price,
                'profit_rate': self.profit_rate,
                'max_profit_rate': self.max_profit_rate, 'min_profit_rate': self.min_profit_rate,
                'profit': self.profit, 'max_profit': self.max_profit, 'min_profit': self.min_profit,
                'now_price': self.now_price, 'max_price': self.max_price, 'min_price': self.min_price,
                'max_profit_time': self.max_profit_time, 'min_profit_time': self.min_profit_time,
                'time': self.time
                }
        await self.db_trade.save_position(data=data)
        return True

    def to_dict(self):
        return {'account_id': self.account.account_id,
                'position_id': self.position_id, 'name': self.name, 'code': self.code,
                'volume': self.volume, 'volume_available': self.volume_available,
                'fee': self.fee, 'price': self.price,
                'profit_rate': self.profit_rate,
                'max_profit_rate': self.max_profit_rate, 'min_profit_rate': self.min_profit_rate,
                'profit': self.profit, 'max_profit': self.max_profit, 'min_profit': self.min_profit,
                'now_price': self.now_price, 'max_price': self.max_price, 'min_price': self.min_price,
                'max_profit_time': self.max_profit_time.strftime(
                    '%Y-%m-%d %H:%M:%S') if self.max_profit_time is not None else None,
                'min_profit_time': self.min_profit_time.strftime(
                    '%Y-%m-%d %H:%M:%S') if self.min_profit_time is not None else None,
                'time': self.time.strftime('%Y-%m-%d %H:%M:%S') if self.time is not None else None
                }

