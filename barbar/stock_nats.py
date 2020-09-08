from barbar.async_nats import AsyncNats
from typing import Dict
import json


class StockNats(AsyncNats):
    def __init__(self, loop, options: Dict = None):
        super().__init__(loop=loop, options=options)

        self.handler_dict = {}

    def add_handler(self, topic_cmd: str, handler: Dict):
        if topic_cmd not in self.handler_dict:
            self.handler_dict[topic_cmd] = handler
        else:
            self.handler_dict[topic_cmd].update(handler)

    async def on_message(self, subject, reply, data):
        handlers = None
        if subject in self.handler_dict:
            handlers = self.handler_dict[subject]

        if handlers is not None:
            await self.on_command(handlers=handlers, subject=subject, reply=reply, data=data)
        else:
            self.log.warning('no handler for subject: {}'.format(subject))

    async def on_command(self, handlers, subject, reply, data):
        resp = None
        try:
            cmd, data = data['cmd'], data['data'] if 'data' in data else None
            if cmd in handlers.keys():
                status, msg, data = await handlers[cmd](data)
                resp = dict(status=status, msg=msg, data=data)
            else:
                self.log.error('handler not found, data={}'.format(data))
        except Exception as e:
            self.log.error(
                'execute command failed: exception={} subject={}, data={} call_stack={}'.format(e, subject, data,
                                                                                                traceback.format_exc()))
            resp = dict(status='FAIL', msg='调用异常', data=None)
        finally:
            if reply != '' and resp is not None:
                js_resp = json.dumps(resp)
                self.log.info("Response a message: '{data}'".format(data=js_resp))
                await self.publish(subject=reply, data=js_resp.encode())

