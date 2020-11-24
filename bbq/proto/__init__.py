from bbq.proto.bbq_pb2 import *
import uuid
import struct
import ctypes


class Packet:

    def __init__(self, cmd: int = 0, body_len: int = 0, sid: str = None, body: str = None):
        self.cmd = cmd
        self.body_len = body_len
        self.sid = str(uuid.uuid4()).replace('-', '') if sid is None else sid
        self.body = body

    @staticmethod
    def head_len() -> int:
        return 40

    @staticmethod
    def body_len(packet: bytes) -> int:
        s_head = struct.Struct('>II32s')
        _, body_len, _ = s_head.unpack(packet)
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
            self.body = str(body, encoding='utf-8')
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
            s_body.pack_into(buf, s_head.size, bytes(self.body, encoding='utf-8'))

        return bytes(buf)

    def __str__(self):
        return 'Packet{{cmd: {}, body_len: {}, sid: {}, body: {}}}'.format(self.cmd, self.body_len, self.sid, self.body)
