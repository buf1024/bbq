from bbq.proto.my_cmd_tab import *
import uuid
import struct
import ctypes


class Packet:
    def __init__(self, cmd: int = 0, sid: str = None, body: bytes = None):
        self.cmd = cmd
        self.body_len = 0 if body is None else len(body)
        self.sid = str(uuid.uuid4()).replace('-', '') if sid is None else sid
        self.body = body

    @staticmethod
    def head_len() -> int:
        return 40

    @staticmethod
    def body_len(packet: bytes) -> int:
        s_head = struct.Struct('>II32s')
        head_packet = packet[:Packet.head_len()]
        _, body_len, _ = s_head.unpack(head_packet)
        return body_len

    @staticmethod
    def is_complete(packet: bytes) -> bool:
        byte_len = len(packet)
        if byte_len < Packet.head_len():
            return False
        if byte_len < Packet.head_len() + Packet.body_len(packet):
            return False
        return True

    def parse(self, packet: bytes) -> bool:
        s_head = struct.Struct('>II32s')
        head, body = packet[:Packet.head_len()], packet[Packet.head_len():]
        self.cmd, self.body_len, self.sid = s_head.unpack(head)
        self.sid = str(self.sid, encoding='utf-8')
        if self.body_len > 0:
            s_body = struct.Struct('{}s'.format(self.body_len))
            body, = s_body.unpack(body)
            # self.body = str(body, encoding='utf-8')
            self.body = body
        return True

    def serialize(self) -> bytes:
        s_head = struct.Struct('>II32s')
        s_body = None
        if self.body_len > 0:
            s_body = struct.Struct('{}s'.format(self.body_len))
        size = s_head.size + (s_body.size if s_body is not None else 0)
        buf = ctypes.create_string_buffer(size)
        s_head.pack_into(buf, 0, self.cmd, self.body_len, bytes(self.sid, encoding='utf-8'))
        if s_body is not None:
            s_body.pack_into(buf, s_head.size, self.body)

        return bytes(buf)

    def pb_obj(self):
        if self.cmd not in cmd_class:
            return None
        cls = cmd_class[self.cmd]

        inst = cls()
        inst.ParseFromString(self.body)
        return inst

    def __str__(self):
        obj = self.pb_obj()
        obj_str = ''
        if obj is not None:
            s = '{}'.format(obj).split('\n')
            obj_str = ', '.join(s)
            if obj_str.endswith(', '):
                obj_str = obj_str[:-2]
        return 'Packet{{\n  header: cmd: {}(0x{:06x}), body_len: {}, sid: {} \n  body: {{ {} }} \n}}'.format(
            cmd_name[self.cmd], self.cmd, self.body_len, self.sid, obj_str)


if __name__ == '__main__':
    p = QuotSubscribeReq()
    p.type = 'rt'
    p.frequency = '1m'

    s = p.SerializeToString()
    print('s: {}'.format(s))

    packet = Packet(cmd=quot_subscribe_req, body=s)
    print(packet)

    b = packet.serialize()

    packet = Packet()
    packet.parse(b)
    print(packet)
