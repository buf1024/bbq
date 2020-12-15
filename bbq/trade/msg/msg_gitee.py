import aiohttp
from typing import Optional, List, Dict
import asyncio
import json
import bbq.log as log


class MsgGitee:
    headers = {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/73.0.3683.86 Safari/537.36',
        'Content-Type': 'application/json;charset=UTF-8',
    }

    def __init__(self):
        self.log = log.get_logger(self.__class__.__name__)

        self.token = None
        self.api = {
            'list_issues': 'https://gitee.com/api/v5/repos/{owner}/{repo}/issues',
            'create_issue': 'https://gitee.com/api/v5/repos/{owner}/issues',
            'update_issue': 'https://gitee.com/api/v5/repos/{owner}/issues/{number}',
            'list_issue_comment': 'https://gitee.com/api/v5/repos/{owner}/{repo}/issues/{number}/comments',
            'create_comment': 'https://gitee.com/api/v5/repos/{owner}/{repo}/issues/{number}/comments',
            'update_comment': 'https://gitee.com/api/v5/repos/{owner}/{repo}/issues/comments/{id}',
        }

    def init_gitee(self, token):
        self.token = token
        return True

    async def retry_http_do(self, method, url, data=None):
        for i in range(3):
            try:
                async with aiohttp.ClientSession() as session:
                    async with session.request(method=method, url=url, data=data, headers=self.headers) as req:
                        js = await req.json()
                        if isinstance(js, dict):
                            if 'message' in js:
                                self.log.error('request={}, error={}'.format(url, js['message']))
                                return None
                        return js
            except aiohttp.ClientConnectorError as e:
                print('连接错误, url={}, 第{}次重试'.format(url, i + 1))
                await asyncio.sleep(5 * (i + 1))
                continue
            except aiohttp.ClientOSError as e:
                print('连接错误, url={}, 第{}次重试'.format(url, i + 1))
                await asyncio.sleep(5 * (i + 1))
                continue
        return None

    async def _loop_get(self, base_url, base_param):
        page = 1
        per_page = 100

        ret_data = []
        while True:
            url = '{}?{}&page={}&per_page={}'.format(base_url, base_param, page, per_page)

            js = await self.retry_http_do(method='GET', url=url)
            if js is None:
                break

            for data in js:
                ret_data.append(data)

            if per_page == len(js):
                page += 1
            else:
                break

        return ret_data

    async def list_issues(self, owner, repo, status='open') -> Optional[List[Dict]]:
        base_url = self.api['list_issues'].format(owner=owner, repo=repo)
        base_param = 'access_token={}&status={}'.format(self.token, status)

        return await self._loop_get(base_url=base_url, base_param=base_param)

    async def create_issue(self, owner, repo, title, body, labels=None) -> Optional[Dict]:
        base_url = self.api['create_issue'].format(owner=owner)
        data = json.dumps(dict(access_token=self.token, owner=owner, repo=repo,
                               title=title, body=body,
                               labels=','.join(labels) if labels is not None else ''))
        return await self.retry_http_do(method='POST', url=base_url, data=data)

    async def update_issue(self, owner, repo, number, title=None, body=None, labels=None, state='closed'):
        base_url = self.api['update_issue'].format(owner=owner, number=number)
        base_dict = dict(access_token=self.token, owner=owner, repo=repo, number=number)
        if title is not None:
            base_dict.update(dict(title=title))
        if body is not None:
            base_dict.update(dict(body=body))
        if state is not None:
            base_dict.update(dict(state=state))
        if labels is not None:
            base_dict.update(dict(labels=','.join(labels) if labels is not None else ''))
        data = json.dumps(base_dict)
        return await self.retry_http_do(method='PATCH', url=base_url, data=data)

    async def update_comment(self, owner, repo, comment_id, body):
        base_url = self.api['update_comment'].format(owner=owner, repo=repo, id=comment_id)
        data = json.dumps(dict(access_token=self.token, owner=owner, repo=repo,
                               id=comment_id, body=body))
        return await self.retry_http_do(method='PATCH', url=base_url, data=data)

    async def list_issue_comment(self, owner, repo, number):
        base_url = self.api['list_issue_comment'].format(owner=owner, repo=repo, number=number)
        base_param = 'access_token={}&order=asc'.format(self.token)
        return await self._loop_get(base_url=base_url, base_param=base_param)

    async def create_comment(self, owner, repo, number, body) -> Optional[Dict]:
        base_url = self.api['create_comment'].format(owner=owner, repo=repo, number=number)
        data = json.dumps(dict(access_token=self.token, owner=owner, repo=repo,
                               number=number, body=body))
        return await self.retry_http_do(method='POST', url=base_url, data=data)


if __name__ == '__main__':
    from bbq.common import run_until_complete


    async def test_msg():
        gitee = MsgGitee()
        gitee.init_gitee(token='a61f482097e041c30e05327fb8b0a2ae')

        # issue = await gitee.list_issues('heidonglgc', 'bbq-broker')
        # print(issue)

        issue = await gitee.create_issue('heidonglgc', 'bbq-broker', '买入委托(神算子4号)',
                                         '```yaml\nname:太平洋\ncode:sh601099\nprice:5.44\nvolume=800\n```',
                                         labels=['买入委托'])
        print(issue)

        # comment = await gitee.list_issue_comment('heidonglgc', 'bbq-broker', 'I2995R')
        # print(comment)

        # issue = await gitee.update_issue('heidonglgc', 'bbq-broker', 'I2995R')
        # print(issue)

        comment = await gitee.create_comment('heidonglgc', 'bbq-broker', 'I2998W',
                                             '> ```yaml\n> name:太平洋\n> code:sh601099\n> price:5.44\n> volume=800\n> ```\n\n@heidonglgc #id 处理失败')
        print(comment)


    run_until_complete(
        # test_wechat()
        test_msg()
    )
