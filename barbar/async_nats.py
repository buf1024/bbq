import asyncio
import traceback
import barbar.log as log
from threading import Thread
from nats.aio.client import Client as NATS
from typing import Dict
import json


class AsyncNats(Thread):
    def __init__(self, loop, options: Dict = None):
        super().__init__(daemon=True)
        self.log = log.get_logger(self.__class__.__name__)

        self.options = options
        self.nc = NATS()

        self.subjects = {}

        self.loop = loop

    async def on_nats_error(self, e):
        self.log.error('Nat error: '.format(e))

    async def on_nats_closed(self):
        self.log.info("Connection to NATS is closed.")
        await asyncio.sleep(0.1, loop=self.loop)

    async def on_nats_connected(self):
        self.log.info("Connected to NATS at {} success.".format(self.nc.connected_url.netloc))

    async def on_nats_reconnected(self):
        self.log.info("Connected to NATS at {}...".format(self.nc.connected_url.netloc))

    async def on_nats_message(self, msg):
        subject = msg.subject
        reply = msg.reply
        data = msg.data.decode()
        self.log.info("Received a message on '{subject} {reply}': {data}".format(
            subject=subject, reply=reply, data=data))
        data = json.loads(data)

        await self.on_message(subject=subject, reply=reply, data=data)

    async def on_message(self, subject, reply, data):
        pass

    def stop(self):
        if self.nc.is_closed:
            return
        self.log.info("Disconnecting...")
        self.loop.create_task(self.nc.close())

    async def nats_task(self):
        options = {
            'io_loop': self.loop,
            'error_cb': self.on_nats_error,
            'closed_cb': self.on_nats_closed,
            'reconnected_cb': self.on_nats_reconnected,
            'servers': ['nats://127.0.0.1:4222'],
            'no_echo': True
        }
        if self.options is not None:
            options.update(self.options)

        try:
            await self.nc.connect(**options)
        except Exception as e:
            self.log.error('connect to error: exception={}, servers={}, call stack={}'.format(e, options['servers'],
                                                                                              traceback.format_exc()))
            return False

        self.log.info('Connected to NATS at {}...'.format(self.nc.connected_url.netloc))

        await self.on_nats_connected()

        return True

    async def subscribe(self, subject: str):
        if not self.nc.is_closed:
            if subject not in self.subjects:
                ssid = await self.nc.subscribe(subject=subject, cb=self.on_nats_message)
                self.subjects[subject] = ssid

    async def unsubscribe(self, subject: str):
        if not self.nc.is_closed:
            if subject in self.subjects:
                await self.nc.unsubscribe(ssid=self.subjects[subject])
                del self.subjects[subject]

    async def publish(self, subject: str, data: object):
        if not self.nc.is_closed:
            payload = ''
            if isinstance(data, dict):
                payload = json.dumps(data).encode()
            else:
                payload = str(data).encode()
            await self.nc.publish(subject=subject, payload=payload)
            await self.nc.flush()
