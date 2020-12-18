from .robot import Robot
from bbq.trade.msg.msg_gitee import MsgGitee
from bbq.trade.account import Account
from typing import Dict
import asyncio
from datetime import datetime


class RobotGitee(Robot):
    def __init__(self, robot_id, account: Account):
        super().__init__(robot_id=robot_id, account=account)
        self.owner = ''
        self.repo = ''
        self.timeout = 3

        self.issue_url = 'https://gitee.com/{owner}/{repo}/issues/{number}'

        self.msg_gitee: MsgGitee = None

        self.task_running = False

    def name(self):
        return 'Gitee运维机器人'

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
            self.task_running = True
            await self.trader.task_queue.put('gitee_robot_task')
            self.trader.loop.create_task(self.gitee_poll_task())
            self.task_running = True

    async def notify_error_comment(self, issue, labels, text):
        body = issue['body'] + '\n\n@{} {} 已处理失败: {}'.format(self.owner,
                                                             datetime.now().strftime('%Y-%m-%d %H:%M:%S'), text)

        await self.msg_gitee.create_comment(owner=self.owner, repo=self.repo,
                                            number=issue['number'], body=body)
        labels.append('处理失败')
        await self.msg_gitee.update_issue(owner=self.owner, repo=self.repo,
                                          number=issue['number'], labels=labels)

    async def poll_event(self):
        issues = await self.msg_gitee.list_issues(owner=self.owner, repo=self.repo)
        for issue in issues:
            labels = self.msg_gitee.get_labels(issue)

            try:
                js = self.msg_gitee.parse_content(issue['body'])
                event = js['event']
                if event not in ['evt_status_report', 'evt_trade_report']:
                    await self.notify_error_comment(issue=issue, labels=labels,
                                                    text='event={} 有误，检查之(evt_trade_report/evt_status_report)'.format(
                                                        event))
                    return
                if event == 'evt_status_report' or event == 'evt_trade_report':
                    body = issue['body'] + '\n\n@{} {} 已处理成功'.format(self.owner,
                                                                     datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
                    await self.msg_gitee.create_comment(owner=self.owner, repo=self.repo,
                                                        number=issue['number'], body=body)
                    labels.append('处理成功')
                    await self.msg_gitee.update_issue(owner=self.owner, repo=self.repo,
                                                      number=issue['number'], labels=labels)
                    await self.emit('robot', event, None)
                    return

            except Exception as e:
                self.log.error('{}/{} yml={}格式错误, ex={}'.format(self.owner, self.repo, issue['body'], e))
                await self.notify_error_comment(issue=issue, labels=labels, text='yaml格式异常: {}'.format(e))

    async def gitee_poll_task(self):
        self.trader.incr_depend_task('account')
        await self.trader.task_queue.get()
        while self.trader.is_running('robot_gitee_poll'):
            await self.poll_event()
            await asyncio.sleep(self.timeout)

        self.trader.decr_depend_task('account')
        self.trader.task_queue.task_done()
