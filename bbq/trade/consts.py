# event
# 行情事件
# 程序开始 / 交易日早市开始 / 交易日午市开始
evt_start = 'evt_start'
evt_morning_start = 'evt_morning_start'
evt_noon_start = 'evt_noon_start'

# 行情下发
evt_quotation = 'evt_quotation'

# 程序结束/交易日早市结束/交易日午市结束
evt_end = 'evt_end'
evt_morning_end = 'evt_morning_end'
evt_noon_end = 'evt_noon_end'

# 委托事件 payload = Entrust
# 委托broker事件: 买/卖/取消
evt_entrust_buy = 'evt_entrust_buy'
evt_entrust_sell = 'evt_entrust_sell'
evt_entrust_cancel = 'evt_entrust_cancel'

# broker产生的反馈事件:
# 委托提交事件/委托(买, 卖)成交事件/撤销事件/资金同步事件/持仓同步事件
evt_broker_committed = 'evt_broker_committed'
evt_broker_deal = 'evt_broker_deal'
evt_broker_cancelled = 'evt_broker_cancelled'
evt_broker_fund_sync = 'evt_broker_fund_sync'
evt_broker_pos_sync = 'evt_broker_pos_sync'

# 交易信号事件
# 取消委托/委托买/委托卖
evt_sig_cancel = 'evt_sig_cancel'
evt_sig_buy = 'evt_sig_buy'
evt_sig_sell = 'evt_sig_sell'

# 内部终止事件
evt_term = '__evt_term'


# action
act_buy = 'buy'
act_sell = 'sell'
act_cancel = 'cancel'
