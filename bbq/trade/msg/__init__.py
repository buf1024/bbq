from .msg_push import MsgPush
from .msg_gitee import MsgGitee

_msg_push = MsgPush()


init_push = _msg_push.init_push
send_email = _msg_push.send_email
wechat_push = _msg_push.wechat_push


_gitee = MsgGitee()

init_gitee = _gitee.init_gitee
list_issues = _gitee.list_issues
create_issue = _gitee.create_issue
update_issue = _gitee.update_issue
list_issue_comment = _gitee.list_issue_comment
create_comment = _gitee.create_comment
