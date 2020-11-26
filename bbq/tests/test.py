from bbq.proto import *

if __name__ == '__main__':
    import asyncio


    async def tcp_echo_client(message):
        reader, writer = await asyncio.open_connection(
            '127.0.0.1', 37017)

        data = QuotSubscribeReq()
        data.type = 'hello'
        req = Packet(quot_subscribe_req, body=data.SerializeToString())
        print('Send: {}'.format(req))
        writer.write(req.serialize())

        print('Send: {}'.format(req))
        writer.write(req.serialize())

        data = await reader.read(40)
        rsp = Packet()
        rsp.parse(data)
        print('Received: {}'.format(rsp))

        print('Close the connection')
        writer.close()


    asyncio.run(tcp_echo_client('Hello World!'))
