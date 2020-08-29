from functools import wraps
from collections import namedtuple
import motor.motor_asyncio
import time
import traceback
import pandas as pd
from barbar import log
from abc import ABC


class MongoDB(ABC):
    _MongoStat = namedtuple('_MongoStat', ['client', 'count', 'last'])

    def __init__(self, uri='mongodb://localhost:27017/', pool=5):
        self.log = log.get_logger(self.__class__.__name__)
        self.clients = []

        self.uri = uri
        self.pool = pool

    def _best_client(func):
        @wraps(func)
        def wrapper(self, *args, **kwargs):
            self.clients = sorted(self.clients, key=lambda stat: (stat.count, -stat.last))
            stat_client = self.clients[0]
            self.clients[0] = stat_client._replace(count=stat_client.count + 1, last=time.time())

            kwargs['__client'] = stat_client.client
            return func(self, *args, **kwargs)

        return wrapper

    def init(self):
        try:
            for _ in range(self.pool):
                client = motor.motor_asyncio.AsyncIOMotorClient(self.uri)
                self.clients.append(self._MongoStat(client=client, count=0, last=time.time()))
        except Exception as e:
            self.log.error('连接mongodb失败: uri={}, ex={}'.format(self.uri, e))
            return False

        return True

    @_best_client
    def get_client(self, **kwargs):
        return kwargs['__client'] if '__client' in kwargs else None

    async def do_load(self, coll, filter=None, projection=None, skip=0, limit=0, sort=None, to_frame=True):
        try:
            cursor = coll.find(filter=filter, projection=projection, skip=skip, limit=limit, sort=sort)
            if cursor is not None:
                # data = [await item async for item in cursor]
                data = await cursor.to_list(None)
                cursor.close()
                if to_frame:
                    df = pd.DataFrame(data=data, columns=projection)
                    if not df.empty:
                        if '_id' in df.columns:
                            df.drop(columns=['_id'], inplace=True)
                        return df
                else:
                    return data
            return None
        except:
            self.log.error('mongodb 调用 %s 异常:\n%s\n', self.do_load.__name__, traceback.format_exc())

    async def do_update(self, coll, filter=None, update=None):
        try:
            if update is None:
                return None
            res = await coll.update_one(filter, {'$set': update}, upsert=True)
            return res.upserted_id
        except:
            self.log.error('mongodb 调用 %s 异常:\n%s\n', self.do_update.__name__, traceback.format_exc())

    async def do_batch_update(self, data, func):
        upsert_list = []
        for item in data.to_dict('records'):
            coll, filter, update = func(item)
            upsert = await self.do_update(coll, filter=filter, update=update)
            if upsert is None:
                continue
            if isinstance(upsert, list):
                upsert_list = upsert_list + upsert
            else:
                upsert_list.append(upsert)
        return upsert_list if len(upsert_list) > 0 else None

    async def do_delete(self, coll, filter=None, just_one=True):
        try:
            res = await coll.remove(filter, {'justOne': just_one})
            return res.upserted_id
        except:
            self.log.error('mongodb 调用 %s 异常:\n%s\n', self.do_delete.__name__, traceback.format_exc())
