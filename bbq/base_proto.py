import asyncio
from bbq.proto import *
from functools import partial
import bbq.log as log


class BaseProto(asyncio.Protocol):
    def __init__(self):
        self.transport = None
        self.packet = bytearray()
        self.tasks = {}
        self.log = log.get_logger(self.__class__.__name__)

    def connection_made(self, transport):
        self.transport = transport

    def data_received(self, data):
        self.packet.extend(data)
        if Packet.is_complete(self.packet):
            packet_len = Packet.body_len(self.packet) + Packet.head_len()
            packet = self.packet[:packet_len]
            self.packet = self.packet[packet_len:]
            try:
                req = Packet()
                req.parse(packet)

                if req.cmd == heartbeat_req:
                    rsp = Packet(cmd=req.cmd + 1, sid=req.sid)
                    bs = rsp.serialize()
                    self.transport.write(bs)
                else:
                    loop = asyncio.get_event_loop()
                    task = loop.create_task(self.handle(self, req))
                    task.add_done_callback(partial(self.done_callback, req.sid))
                    self.tasks[req.sid] = task

            except Exception as e:
                self.log.error('处理报文异常: {}'.format(e))

    def done_callback(self, sid, *arg):
        del self.tasks[sid]

    def connection_lost(self, exc):
        for sid, task in self.tasks.items():
            task.cancel()
        self.transport = None

    def write(self, data: bytes):
        if self.transport is not None:
            self.transport.write(data)

    async def handle(self, proto, req):
        self.log.info('req: {}'.format(req))


class BaseClient:
    def __init__(self):
        pass


if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    server = loop.run_until_complete(loop.create_server(BaseProto, '127.0.0.1', 37017))
    print('server running')
    loop.run_until_complete(server.wait_closed())
