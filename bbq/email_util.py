import poplib
from concurrent.futures import ThreadPoolExecutor, wait
from datetime import datetime
from email.header import decode_header
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.parser import Parser
from email.utils import parseaddr
import asyncio
import aiosmtplib
import queue


class EmailUtil:
    def __init__(self, user, passwd, pop_host=None, smtp_host=None, pop_port=995, smtp_port=465):
        self.pop_host = pop_host
        self.pop_port = pop_port
        self.smtp_host = smtp_host
        self.smtp_port = smtp_port
        self.user = user
        self.passwd = passwd

    def recv_email(self, start_date: datetime = None, filter_from: str = None, filter_sub: str = None):
        if self.pop_host is None or self.pop_port <= 0:
            return None

        def _parser_from(msg):
            _, addr = parseaddr(msg['From'])
            # name, charset = decode_header(hdr)[0]
            # if charset:
            #     name = name.decode(charset)
            return addr

        def _parser_to(msg):
            _, addr = parseaddr(msg['To'])
            return addr

        def _parser_subject(msg):
            subject = msg['Subject']
            subject, charset = decode_header(subject)[0]
            if charset:
                subject = subject.decode(charset)
            return subject

        def _parse_date(msg):
            dat = msg['Date'][:-6]
            return datetime.strptime(dat, '%a, %d %b %Y %H:%M:%S')

        def _parse_content(msg):
            if msg is None:
                return None
            for part in msg.walk():
                if not part.is_multipart():
                    data = part.get_payload(decode=True)
                    return data.decode()

            return None

        server = poplib.POP3_SSL(self.pop_host, self.pop_port)
        server.user(self.user)
        server.pass_(self.passwd)
        _, msg_list, _ = server.list()
        msgs = []
        for r in range(len(msg_list), 0, -1):
            _, msglines, _ = server.retr(r)
            msg_content = b'\r\n'.join(msglines).decode('utf-8')
            msg_content = Parser().parsestr(text=msg_content)
            msg = {
                'From': _parser_from(msg_content),
                'To': _parser_to(msg_content),
                'Subject': _parser_subject(msg_content),
                'Content': _parse_content(msg_content),
                'Date': _parse_date(msg_content)
            }
            if start_date is not None:
                dat = msg['Date']
                if dat is not None and dat < start_date:
                    break
            if filter_from is not None:
                fr = msg['From']
                if fr != filter_from:
                    continue
            if filter_sub is not None and len(filter_sub) > 0:
                sub = msg['Subject']
                if filter_sub not in sub:
                    continue

            msgs.append(msg)

        server.close()
        return msgs

    async def send_email(self, email, title, content):
        if self.smtp_host is None or self.smtp_port <= 0:
            return None

        message = MIMEMultipart('alternative')
        message['From'] = self.user
        message['To'] = email
        message['Subject'] = title

        html_message = MIMEText(content, 'html', 'utf-8')
        message.attach(html_message)
        return await aiosmtplib.send(message, hostname=self.smtp_host, port=self.smtp_port,
                                     username=self.user, password=self.passwd, use_tls=True)


class EmailUtilAsync(EmailUtil):
    def __init__(self, user, passwd, pop_host, smtp_host, pop_port=995, smtp_port=465):
        super().__init__(user, passwd, pop_host, smtp_host, pop_port, smtp_port)

        self.executor = ThreadPoolExecutor(max_workers=2)
        self.lock_queue = asyncio.Queue(1)
        self.res_queue = queue.Queue(1)

    async def recv_email(self, start_date: datetime = None, filter_from: str = None, filter_sub: str = None):
        await self.lock_queue.put('thread_task')
        self.executor.submit(self._recv_email,
                             start_date=start_date, filter_from=filter_from, filter_sub=filter_sub)
        msgs = None
        while True:
            try:
                msgs = self.res_queue.get(block=False)
                self.res_queue.task_done()
                break
            except queue.Empty:
                await asyncio.sleep(3)
        await self.lock_queue.get()
        self.lock_queue.task_done()
        return msgs

    def _recv_email(self, start_date: datetime = None, filter_from: str = None, filter_sub: str = None):
        msgs = super().recv_email(start_date=start_date, filter_from=filter_from, filter_sub=filter_sub)
        self.res_queue.put(msgs)


if __name__ == '__main__':
    from bbq.common import run_until_complete
    from bbq.trade import Entrust

    util = EmailUtilAsync(pop_host='pop.126.com', smtp_host='smtp.126.com', user='barbarianquant@126.com',
                          passwd='VOBPMUWKNJOZCNNI')


    async def _test1():
        emails = await util.recv_email(filter_sub='fuck')
        print(emails)

    async def _test2():
        emails = await util.recv_email(filter_sub='马日入')
        print(emails)

    async def send_entrust():
        class Account:
            def __init__(self) -> None:
                self.account_id = 123
                self.typ = None
                self.db_data = None
                self.db_trade = None
                self.trader = None
        entrust = Entrust('123', Account())
        entrust.name = '股票名称'
        entrust.code = 'sh600063'
        entrust.time = datetime.now()

        entrust.broker_entrust_id = '112333'  # broker对应的委托id
        entrust.type = 'buy'  # buy, sell, cancel
        entrust.status = Entrust.stat_init  # init, 初始化 commit 已提交 deal 已成 part_deal 部成 cancel 已经取消

        entrust.price = 1.0  # 价格
        entrust.volume = 10  # 量

        entrust.volume_deal = 0  # 已成量
        entrust.volume_cancel = 0  # 已取消量

        entrust.desc = '测试'
        d = entrust.to_dict()
        s = ''
        for k, v in enumerate(d):
            s = '{s}{k}: {v}br\n'.format(s=s, k=k, v=v)
        body = s
        print(datetime.now())
        await util.send_email('barbarianquant@126.com', title='马日入', content=body)
        print(datetime.now())
        print('done')


    run_until_complete(send_entrust())
