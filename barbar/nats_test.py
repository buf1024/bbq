import asyncio
import signal
import sys
import json
import uuid
from nats.aio.client import Client as NATS


async def test(loop, nc, handler):
    payload = {
        'db': {
            "uri": "mongodb://localhost:37017/",
            "pool": 5
        },
        'broker': {
            'name': 'BrokerBacktest',
            'account': {
                'cash_init': 80000.00,
                'cash_available': 80000.00
            },
            'strategy': {
                'name': 'Dummy'
            },
            'risk': {
                'name': 'SimpleStop'
            }
        },
        'quot': {
            'name': 'QuotBacktest',
            'start_date': '20191225',
            'end_date': '20200102',
            'interval': 0
        }
    }
    command = {
        'id': str(uuid.uuid4()),
        'cat': 'backtest',
        'cmd': 'run',
        'options': payload
    }
    print('request: {}'.format(command))
    msg = await nc.request('topic:barbarian.command', json.dumps(command).encode(), timeout=60)
    print('response: {}'.format(msg.data))


async def run(loop):
    nc = NATS()

    async def error_cb(e):
        print("Error:", e)

    async def closed_cb():
        print("Connection to NATS is closed.")
        await asyncio.sleep(0.1, loop=loop)
        loop.stop()

    async def reconnected_cb():
        print("Connected to NATS at {}...".format(nc.connected_url.netloc))

    async def subscribe_handler(msg):
        subject = msg.subject
        reply = msg.reply
        data = msg.data.decode()
        print("Received a message on '{subject} {reply}': {data}".format(
            subject=subject, reply=reply, data=data))
        await nc.publish(reply, b'response')

    options = {
        "io_loop": loop,
        "error_cb": error_cb,
        "closed_cb": closed_cb,
        "reconnected_cb": reconnected_cb
    }

    try:
        await nc.connect(**options)
    except Exception as e:
        print(e)
        sys.exit(-1)

    print("Connected to NATS at {}...".format(nc.connected_url.netloc))

    def signal_handler():
        if nc.is_closed:
            return
        print("Disconnecting...")
        loop.create_task(nc.close())

    for sig in ('SIGINT', 'SIGTERM'):
        loop.add_signal_handler(getattr(signal, sig), signal_handler)

    # await nc.subscribe('hello', cb=subscribe_handler)
    await test(loop, nc, subscribe_handler)


if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.run_until_complete(run(loop))
    try:
        loop.run_forever()
    finally:
        loop.close()
