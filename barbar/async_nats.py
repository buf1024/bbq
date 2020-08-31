import asyncio
import traceback
import barbar.log as log
from threading import Thread
from queue import Queue, Empty
from nats.aio.client import Client as NATS
from typing import Dict, List
import json


class AsyncNats(Thread):
    def __init__(self, options: Dict = None):
        super().__init__(daemon=True)
        self.log = log.get_logger(self.__class__.__name__)

        self.queue_in = None
        self.queue_out = None
        self.options = options
        self.nc = NATS()

        self.subjects = {}
        self.loop = None

        self.running = False
        self.queue_handlers = {
            'subscribe': self._subscribe,
            'unsubscribe': self._unsubscribe,
            'publish': self._publish
        }

    async def on_nats_error(self, e):
        self.log.error('Nat error: '.format(e))

    async def on_nats_closed(self):
        self.log.info("Connection to NATS is closed.")
        await asyncio.sleep(0.1, loop=self.loop)
        self.running = False
        self.loop.stop()

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

        await self.queue_out.put(dict(subject=subject, reply=reply, data=data))

    def stop(self):
        if self.nc.is_closed:
            return
        self.log.info("Disconnecting...")
        self.loop.create_task(self.nc.close())

    async def queue_task(self):
        while self.running:
            try:
                data = await self.queue_in.get()
                self.log.debug('NATS queue recv: {}'.format(data))
                cmd, data = data['cmd'], data['data']

                if cmd in self.queue_handlers.keys():
                    await self.queue_handlers[cmd](data)
                else:
                    self.log.warning('unknown queue data: {}'.format(data))
            except Empty:
                pass

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

    def run(self) -> None:
        try:

            self.loop = asyncio.new_event_loop()
            asyncio.set_event_loop(self.loop)

            self.queue_in = asyncio.Queue(loop=self.loop)
            self.queue_out = asyncio.Queue(loop=self.loop)

            if self.loop.run_until_complete(self.nats_task()):
                self.running = True
                self.loop.create_task(self.queue_task())
                self.loop.run_forever()
        finally:
            self.log.info('nats loop done')

    async def _subscribe(self, data):
        if not self.nc.is_closed:
            subject = data['subject']
            if subject not in self.subjects:
                ssid = await self.nc.subscribe(subject=subject, cb=self.on_nats_message)
                self.subjects[subject] = ssid

    async def _unsubscribe(self, data):
        if not self.nc.is_closed:
            subject = data['subject']
            if subject in self.subjects:
                await self.nc.unsubscribe(ssid=self.subjects[subject])
                del self.subjects[subject]

    async def _publish(self, data):
        if not self.nc.is_closed:
            subject, data = data['subject'], data['data']
            payload = ''
            if isinstance(data, dict):
                payload = json.dumps(data).encode()
            else:
                payload = str(data).encode()
            await self.nc.publish(subject=subject, payload=payload)
            await self.nc.flush()

    def subscribe(self, subject):
        self.queue_in.put_nowait(dict(cmd='subscribe', data=dict(subject=subject)))

    def unsubscribe(self, subject):
        self.queue_in.put_nowait(dict(cmd='unsubscribe', data=dict(subject=subject)))

    def publish(self, subject, data):
        self.log.info('push')
        self.queue_in.put_nowait(dict(cmd='publish', data=dict(subject=subject, data=data)))
