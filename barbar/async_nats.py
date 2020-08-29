import asyncio
import traceback
import barbar.log as log
import signal
from nats.aio.client import Client as NATS


class AsyncNats:
    def __init__(self, loop):
        self.log = log.get_logger(self.__class__.__name__)

        self.nc = NATS()
        self.loop = loop

        self._subjects = {}

    async def on_nats_error(self, e):
        self.log.error('Nat error: '.format(e))

    async def on_nats_closed(self):
        self.log.info("Connection to NATS is closed.")
        await asyncio.sleep(0.1, loop=self.loop)

    async def on_nats_connected(self):
        pass

    async def on_nats_reconnected(self):
        self.log.info("Connected to NATS at {}...".format(self.nc.connected_url.netloc))

    async def on_nats_message(self, msg):
        subject = msg.subject
        reply = msg.reply
        data = msg.data.decode()
        self.log.info("Received a message on '{subject} {reply}': {data}".format(
            subject=subject, reply=reply, data=data))

    def on_signal(self):
        if self.nc.is_closed:
            return
        self.log.info("Disconnecting...")
        self.loop.create_task(self.nc.close())

    async def task(self, options=None):
        def_options = {
            'io_loop': self.loop,
            'error_cb': self.on_nats_error,
            'closed_cb': self.on_nats_closed,
            'reconnected_cb': self.on_nats_reconnected,
            'servers': ['nats://127.0.0.1:4222'],
            'no_echo': True
        }
        if options is not None:
            def_options.update(options)

        try:
            await self.nc.connect(**def_options)
        except Exception as e:
            self.log.error('connect to error: exception={}, servers={}, call stack={}'.format(e, def_options['servers'],
                                                                                              traceback.format_exc()))
            return False

        self.log.info('Connected to NATS at {}...'.format(self.nc.connected_url.netloc))
        # for sig in ('SIGINT', 'SIGTERM'):
        #     self.loop.add_signal_handler(getattr(signal, sig), self.on_signal)

        await self.on_nats_connected()

        return True

    def start(self):
        try:
            if self.loop.run_until_complete(self.task()):
                self.loop.run_forever()
        finally:
            self.log.info('nats loop done')

    async def subscribe(self, subject):
        if not self.nc.is_closed:
            if subject not in self._subjects:
                ssid = await self.nc.subscribe(subject=subject, cb=self.on_nats_message)
                self._subjects[subject] = ssid

    async def unsubscribe(self, subject):
        if not self.nc.is_closed:
            if subject in self._subjects:
                await self.nc.unsubscribe(ssid=self._subjects[subject])
