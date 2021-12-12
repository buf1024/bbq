opendatatools修改:
```python
# opendatatools/stock/stock_agent.py
# L354
if len(jsonobj['data']) <= 0:
    return None, jsonobj['error_description']
for rsp in jsonobj['data']['item']:

# opendatatools/stock/stock_agent.py
# L201
if df is not None and len(df) != 0:

#->


```


bbq -- baibai quantitative(瞎掰掰量化)

用户模型，一个用户对应n个账户，一个账户对应1条交易策略，一条风控策略，一个交易前端(broker)。
user(1)->account(N)->strategy(1)
                   ->risk(1)
                   ->broker(1)
模块划分:

行情模块。
通过rabbitmq进行交互，功能如下
1. 订阅行情。
2. 下发行情。直接丢都队列, account监听队列。account要处理最新的

fetch模块
提供: sina tushare, tdx, ths...等便利api，可能整合通用数据结构

交易模块
交易模块由行情驱动，account订阅和监听行情队列。account注意要处理最新的的行情，避免处理不及时而处理了积压的行情。
broker为券商，登录券商交易端后，需要同步交易信息。
行情到达account后，做常规的更新后，调用对应的策略，策略计算后触发交易信号。
交易信号经过风控拦截计算后，发给broker，同时broker反馈相应结果给account
risk定时监控account,同时发出signal给account，由account发平仓给broker
需要提供人工干预的接口
quotation -> account -> strategy 
                     signal <-> broker 
            signal
             ^
            risk

webserver
模块的统一入口，jupyter分析，行情, websocket通讯

数据存储模块，提供缺失数据及时补全

task executor

             
broker独立docker部署

select模块
          
          
以上独立docker部署。
          

```js
// mongodb 查询慢，需要建索引
db.fund_daily.createIndex({'trade_date': 1})
db.fund_daily.createIndex({'code': 1})
db.fund_daily.createIndex({'trade_date': -1, 'code': 1})

db.fund_net.createIndex({'trade_date': 1})
db.fund_net.createIndex({'code': 1})
db.fund_net.createIndex({'trade_date': -1, 'code': 1})

db.fund_info.createIndex({'code': 1})

db.stock_info.createIndex({'code': 1})

db.stock_margin.createIndex({'code': 1})
db.stock_margin.createIndex({'trade_date': 1})
db.stock_margin.createIndex({'trade_date': -1, 'code': 1})

db.stock_daily.createIndex({'trade_date': 1})
db.stock_daily.createIndex({'code': 1})
db.stock_daily.createIndex({'trade_date': -1, 'code': 1})

db.stock_index.createIndex({'trade_date': 1})
db.stock_index.createIndex({'code': 1})
db.stock_index.createIndex({'trade_date': -1, 'code': 1})

db.stock_fq_factor.createIndex({'trade_date': 1})
db.stock_fq_factor.createIndex({'code': 1})
db.stock_fq_factor.createIndex({'trade_date': -1, 'code': 1})

db.stock_fq_factor.createIndex({'trade_date': 1})
db.stock_fq_factor.createIndex({'code': 1})
db.stock_fq_factor.createIndex({'trade_date': -1, 'code': 1})

db.stock_index.createIndex({'code': 1})

db.stock_index_daily.createIndex({'trade_date': 1})
db.stock_index_daily.createIndex({'code': 1})
db.stock_index_daily.createIndex({'trade_date': -1, 'code': 1})
// 后续根据需要再添加
```

