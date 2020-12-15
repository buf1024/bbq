from .broker import Broker
from bbq.trade.account import Account
from bbq.trade.msg.msg_gitee import MsgGitee
from typing import Dict
import asyncio
import copy
import yaml
from bbq.trade.entrust import Entrust
from datetime import datetime


class BrokerGitee(Broker):
    def __init__(self, broker_id, account: Account):
        super().__init__(broker_id=broker_id, account=account)
        self.owner = ''
        self.repo = ''
        self.timeout = 3

        self.entrust = {}

        self.issue_url = 'https://gitee.com/{owner}/{repo}/issues/{number}'

        self.msg_gitee = None

        self.task_running = False

    async def init(self, opt: Dict) -> bool:
        if 'token' not in opt:
            self.log.error('miss gitee token')
            return False
        self.msg_gitee = MsgGitee()
        if not self.msg_gitee.init_gitee(token=opt['token']):
            self.log.error('init gitee failed.')
            return False

        self.owner = opt['owner'] if 'owner' in opt else ''
        self.repo = opt['repo'] if 'repo' in opt else ''
        if self.owner == '' or self.repo == '':
            self.log.error('owner or repo is empty')
            return False

        return True

    async def on_open(self, evt, payload):
        if not self.task_running:
            await self.trader.task_queue.put('gitee_poll_task')
            self.trader.loop.create_task(self.gitee_poll_task())
            self.task_running = True

        self.log.debug('broker gitee on_open, evt={}'.format(evt))
        if evt != 'evt_morning_start' and evt != 'evt_noon_start':
            return
        trade_date = payload['trade_date']
        issues = await self.msg_gitee.list_issues(self.owner, self.repo)
        for issue in issues:
            body = issue['body']
            title = issue['title']
            entrust_trade_date = title.split('@')[1]
            entrust_trade_date = datetime.strptime(entrust_trade_date, '%Y-%m-%d')
            try:
                body = body.replace('```yaml', '')
                body = body.replace('```', '')
                js = yaml.load(body, yaml.FullLoader)
                if js['entrust_id'] not in self.entrust:
                    if entrust_trade_date == trade_date:
                        entrust = Entrust(js['entrust_id'], self.account)
                        entrust.from_dict(data=js)
                        self.entrust[entrust.entrust_id] = entrust
                    else:
                        self.log.debug('close invalid issue')
                        await self.msg_gitee.update_issue(self.owner, self.repo, issue['number'], state='closed')
            except Exception as e:
                self.log.error('on_open {}/{} yml={}格式错误, ex={}'.format(self.owner, self.repo, body, e))

    async def on_close(self, evt, payload):
        self.log.debug('gitee broker on_close, evt={}'.format(evt))
        if evt == 'evt_noon_end':
            for entrust in self.entrust.values():
                await self.msg_gitee.update_issue(owner=self.owner, repo=self.repo,
                                                  number=entrust.broker_entrust_id, state='closed')

            self.entrust.clear()

    async def on_entrust(self, evt, payload):
        entrust = copy.copy(payload)
        if evt == 'evt_broker_buy' or evt == 'evt_broker_sell':
            body = '```yaml\n{}\n```\n\n'.format(entrust)
            trade_date = entrust.time.strftime('%Y-%m-%d')
            title = '买入委托({})@{}'.format(entrust.signal.source,
                                         trade_date)
            labels = ['买入委托']
            if evt == 'evt_broker_sell':
                title = '卖出委托({})@{}'.format(entrust.signal.source,
                                             trade_date)
                labels = ['卖出委托']

            issue = await self.msg_gitee.create_issue(owner=self.owner, repo=self.repo,
                                                      title=title, body=body, labels=labels)
            if issue is None:
                self.log.error('create entrust:{} issue failed'.format(entrust))
                return

            entrust.status = 'commit'
            entrust.broker_entrust_id = issue['number']

            body = '```yaml\n{}\n```\n\n'.format(entrust)
            await self.msg_gitee.update_issue(owner=self.owner, repo=self.repo, number=issue['number'],
                                              body=body, labels=labels, state='open')
            self.entrust[entrust.entrust_id] = entrust

            self.emit('broker_event', 'evt_broker_commit', copy.copy(entrust))
            await self.trader.msg_push.wechat_push(title=title, text=body)

        if evt == 'evt_broker_cancel':
            if entrust.entrust_id in self.entrust:
                del self.entrust[entrust.entrust_id]

            await self.msg_gitee.update_issue(owner=self.owner, repo=self.repo, number=entrust.broker_entrust_id)
            self.emit('broker_event', 'evt_broker_cancel', copy.copy(entrust))

    async def handle_comment(self, entrust, number, comment) -> bool:
        body = comment['body']
        tag = '@{}'.format(self.owner)
        if body.find(tag) != -1:
            return False
        try:
            body = body.replace('```yaml', '')
            body = body.replace('```', '')
            body = body.replace('```', '')
            js = yaml.load(body, yaml.FullLoader)

            labels = ['买入委托']
            if js['type'] == 'sell':
                labels = ['卖出委托']

            if js['status'] == 'cancel':
                entrust.status = js['status']
                entrust.volume_cancel += js['cancel']

                body = comment['body'] + '\n\n@{} {}已处理成功'.format(self.owner,
                                                                  datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
                await self.msg_gitee.update_comment(owner=self.owner, repo=self.repo,
                                                    comment_id=comment['id'], body=body)

                body = '```yaml\n{}\n```\n\n'.format(entrust)
                await self.msg_gitee.update_issue(owner=self.owner, repo=self.repo, number=number,
                                                  body=body, labels=labels, state='open')

                self.emit('broker_event', 'evt_broker_cancel', copy.copy(entrust))
                if entrust.volume_cancel + entrust.volume_deal == entrust.volume:
                    return True
                return False
            if js['status'] == 'part_deal' or js['status'] == 'deal':
                entrust.status = js['status']
                entrust.volume_deal += js['volume_deal']

                body = comment['body'] + '\n\n@{} {}已处理成功'.format(self.owner,
                                                                  datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
                await self.msg_gitee.update_comment(owner=self.owner, repo=self.repo,
                                                    comment_id=comment['id'], body=body)

                body = '```yaml\n{}\n```\n\n'.format(entrust)
                await self.msg_gitee.update_issue(owner=self.owner, repo=self.repo,
                                                  number=number, body=body, state='open')
                self.emit('broker_event', 'evt_broker_deal', copy.copy(entrust))
                if entrust.volume_cancel + entrust.volume_deal == entrust.volume:
                    return True
                return False

        except Exception as e:
            self.log.error('{}/{} yml={}格式错误, ex={}'.format(self.owner, self.repo, body, e))
            body = comment['body'] + '\n\n@{} {}已处理失败: {}'.format(self.owner,
                                                                  datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                                                                  e)
            await self.msg_gitee.update_comment(owner=self.owner, repo=self.repo,
                                                comment_id=comment['id'], body=body)

            title = 'yaml格式解析错误'
            await self.trader.msg_push.wechat_push(title=title, text=body)

            return False

        self.log.error('{}/{} yml={}格式status={} not right'.format(self.owner, self.repo, js['status'], body))

        title = 'yaml内容解析错误'
        body = comment['body'] + '\n\n@{} {}已处理失败: yaml内容解析错误'.format(self.owner,
                                                                      datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
        await self.msg_gitee.update_comment(owner=self.owner, repo=self.repo,
                                            comment_id=comment['id'], body=body)

        await self.trader.msg_push.wechat_push(title=title, text=body)
        return False

    async def gitee_poll_task(self):
        await self.trader.task_queue.get()
        while self.trader.running:
            del_entrust = []
            entrusts = copy.copy(list(self.entrust.values()))
            for entrust in entrusts:
                comments = await self.msg_gitee.list_issue_comment(owner=self.owner, repo=self.repo,
                                                                   number=entrust.broker_entrust_id)
                for comment in comments:
                    self.log.debug('handle comment: {}'.format(comment))
                    is_del = await self.handle_comment(entrust=entrust,
                                                       number=entrust.broker_entrust_id,
                                                       comment=comment)

                    if is_del:
                        del_entrust.append(entrust.entrust_id)
            for entrust_id in del_entrust:
                number = self.entrust[entrust_id].broker_entrust_id
                await self.msg_gitee.update_issue(self.owner, self.repo, number=number, state='closed')
                del self.entrust[entrust_id]
            await asyncio.sleep(self.timeout)

        self.trader.task_queue.task_done()
