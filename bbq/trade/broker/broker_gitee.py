from .broker import Broker
from bbq.trade.account import Account
from bbq.trade.msg.msg_gitee import MsgGitee
from typing import Dict
import asyncio
import copy
from bbq.trade.entrust import Entrust
from datetime import datetime
from bbq.trade import event


class BrokerGitee(Broker):
    """
    委托应答Comment:
    status: cancel, deal, part_deal
    volume_cancel: cancel是必选
    volume_deal: deal/part_deal是必选

    事件Issue:
    事件pos_sync:
    name: ''  # 股票名称
    code: ''  # 股票代码
    time: 'yyyy-mm-dd HH:MM:SS'  # 首次建仓时间
    volume: 0  # 持仓量
    fee: 0.0  # 持仓费用
    price: 0.0  # 平均持仓价
    事件fund_sync:
    cash_init:
    cash_available:
    """

    def __init__(self, broker_id, account: Account):
        super().__init__(broker_id=broker_id, account=account)
        self.owner = ''
        self.repo = ''
        self.timeout = 3

        self.entrust = {}

        self.issue_url = 'https://gitee.com/{owner}/{repo}/issues/{number}'

        self.msg_gitee: MsgGitee = None

        self.task_running = False

    def name(self):
        return '神算子Gitee手工券商'

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
            await self.trader.task_queue.put('gitee_broker_task')
            self.trader.loop.create_task(self.gitee_poll_task())
            self.task_running = True

        self.log.debug('broker gitee on_open, evt={}'.format(evt))
        if evt != event.evt_morning_start and evt != event.evt_noon_start:
            return
        trade_date = payload['trade_date']
        issues = await self.msg_gitee.list_issues(self.owner, self.repo)
        for issue in issues:
            body = issue['body']
            title = issue['title']
            entrust_trade_date = title.split('@')[1]
            entrust_trade_date = datetime.strptime(entrust_trade_date, '%Y-%m-%d')
            try:
                js = self.msg_gitee.parse_content(body)
                if js['entrust_id'] not in self.entrust:
                    if entrust_trade_date == trade_date:
                        entrust = Entrust(js['entrust_id'], self.account)
                        entrust.from_dict(data=js)
                        self.entrust[entrust.entrust_id] = dict(entrust=entrust, issue=issue)
                    else:
                        self.log.debug('close invalid issue')
                        labels = self.msg_gitee.get_labels(issue)
                        labels.append('委托关闭')
                        await self.msg_gitee.update_issue(self.owner, self.repo, issue['number'],
                                                          labels=labels, state='closed')
            except Exception as e:
                self.log.error('on_open {}/{} yml={}格式错误, ex={}'.format(self.owner, self.repo, body, e))

    async def on_close(self, evt, payload):
        self.log.debug('gitee broker on_close, evt={}'.format(evt))
        if evt == event.evt_noon_end:
            for entrust_dict in self.entrust.values():
                entrust = entrust_dict['entrust']
                labels = self.msg_gitee.get_labels(entrust_dict['issue'])
                labels.append('委托关闭')
                await self.msg_gitee.update_issue(owner=self.owner, repo=self.repo,
                                                  number=entrust.broker_entrust_id, state='closed')

            self.entrust.clear()

    async def on_entrust(self, evt, payload):
        entrust = copy.copy(payload)
        if evt == event.evt_broker_buy or evt == event.evt_broker_sell:
            body = '```yaml\n{}\n```\n\n'.format(entrust)
            trade_date = entrust.time.strftime('%Y-%m-%d')
            title = '买入委托({})@{}'.format(entrust.signal.source_name,
                                         trade_date)
            labels = ['买入委托']
            if evt == event.evt_broker_sell:
                title = '卖出委托({})@{}'.format(entrust.signal.source_name,
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
            self.entrust[entrust.entrust_id] = dict(entrust=entrust, issue=issue)

            await self.emit('broker_event', event.evt_broker_committed, copy.copy(entrust))
            await self.trader.msg_push.wechat_push(title=title, text=body)

        if evt == event.evt_broker_cancel:
            labels = []
            if entrust.entrust_id in self.entrust:
                issue = self.entrust[entrust.entrust_id]['entrust']
                labels = self.msg_gitee.get_labels(issue)
            labels.append('取消委托')
            await self.msg_gitee.update_issue(owner=self.owner, repo=self.repo,
                                              number=entrust.broker_entrust_id,
                                              labels=labels)
            await self.emit('broker_event', event.evt_broker_cancel, copy.copy(entrust))

    async def notify_error_comment(self, comment, text):
        body = '{}\n\n@{} {} 处理失败: {}'.format(comment['body'], self.owner,
                                              datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                                              text)
        await self.msg_gitee.update_comment(owner=self.owner, repo=self.repo,
                                            comment_id=comment['id'], body=body)

        title = 'Comment 处理失败'
        await self.trader.msg_push.wechat_push(title=title, text=body)

    async def handle_comment(self, entrust, issue, number, comment) -> bool:
        body = comment['body']
        tag = '@{}'.format(self.owner)
        if body.find(tag) != -1:
            return False
        try:
            js = self.msg_gitee.parse_content(body)

            status = js['status']
            if status not in ['part_deal', 'deal', 'cancel']:
                await self.notify_error_comment(comment, 'status={} 有误，检查之(非part_deal/deal/cancel)'.format(status))
                return False
            labels = self.msg_gitee.get_labels(issue)
            if status == 'cancel':
                entrust.status = status
                volume = js['volume_cancel']
                entrust.volume_cancel += volume
                if entrust.volume_cancel + entrust.volume_deal > entrust.volume:
                    await self.notify_error_comment(comment, 'volume_cancel={} 有误，检查之(大于委托总量)'.format(volume))
                    return False

                body = comment['body'] + '\n\n@{} {} 已处理成功'.format(self.owner,
                                                                   datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
                await self.msg_gitee.update_comment(owner=self.owner, repo=self.repo,
                                                    comment_id=comment['id'], body=body)

                body = '```yaml\n{}\n```\n\n'.format(entrust)
                labels.append('委托取消')
                issue = await self.msg_gitee.update_issue(owner=self.owner, repo=self.repo, number=number,
                                                          body=body, labels=labels, state='open')
                self.entrust[entrust.entrust_id]['entrust'] = entrust
                self.entrust[entrust.entrust_id]['issue'] = issue

                await self.emit('broker_event', event.evt_broker_cancel, copy.copy(entrust))
                if entrust.volume_cancel + entrust.volume_deal == entrust.volume:
                    labels.append('委托完成')
                    await self.msg_gitee.update_issue(self.owner, self.repo, number=number, labels=labels,
                                                      state='closed')
                    del self.entrust[entrust.entrust_id]
                    return True
                return False
            if status == 'part_deal' or status == 'deal':
                entrust.status = js['status']
                volume = js['volume_deal']
                entrust.volume_deal += volume
                if entrust.volume_cancel + entrust.volume_deal > entrust.volume:
                    await self.notify_error_comment(comment, 'volume_deal={} 有误，检查之(大于委托总量)'.format(volume))
                    return False

                body = comment['body'] + '\n\n@{} {} 已处理成功'.format(self.owner,
                                                                   datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
                await self.msg_gitee.update_comment(owner=self.owner, repo=self.repo,
                                                    comment_id=comment['id'], body=body)

                body = '```yaml\n{}\n```\n\n'.format(entrust)
                if js['status'] == 'part_deal':
                    labels.append('部分成交')
                else:
                    labels.append('全部成交')
                issue = await self.msg_gitee.update_issue(owner=self.owner, repo=self.repo,
                                                          number=number, body=body,
                                                          labels=labels, state='open')
                self.entrust[entrust.entrust_id]['entrust'] = entrust
                self.entrust[entrust.entrust_id]['issue'] = issue
                await self.emit('broker_event', event.evt_broker_deal, copy.copy(entrust))
                if entrust.volume_cancel + entrust.volume_deal == entrust.volume:
                    labels.append('委托完成')
                    await self.msg_gitee.update_issue(self.owner, self.repo, number=number,
                                                      labels=labels,
                                                      state='closed')
                    del self.entrust[entrust.entrust_id]
                    return True
                return False

        except Exception as e:
            self.log.error('{}/{} yml={}格式错误, ex={}'.format(self.owner, self.repo, body, e))
            await self.notify_error_comment(comment, 'yaml格式异常: {}'.format(e))

        return False

    async def poll_entrust(self):
        entrusts = copy.copy(list(self.entrust.values()))
        for entrust_dict in entrusts:
            entrust, issue = entrust_dict['entrust'], entrust_dict['issue']
            comments = await self.msg_gitee.list_issue_comment(owner=self.owner, repo=self.repo,
                                                               number=entrust.broker_entrust_id)
            for comment in comments:
                self.log.debug('handle comment: {}'.format(comment))
                await self.handle_comment(entrust=entrust, issue=issue,
                                          number=entrust.broker_entrust_id,
                                          comment=comment)

    async def poll_event(self):
        issues = await self.msg_gitee.list_issues(owner=self.owner, repo=self.repo)
        for issue in issues:
            title = issue['title']
            if '事件' in title:
                pass

    async def gitee_poll_task(self):
        self.trader.incr_depend_task('broker_event')
        await self.trader.task_queue.get()
        while self.trader.is_running('broker_gitee_poll'):
            await self.poll_entrust()
            # await self.poll_event()

            await asyncio.sleep(self.timeout)

        self.trader.decr_depend_task('broker_event')
        self.trader.task_queue.task_done()
