### fund_db 通用数据库

#### fund_plate_coll 基金板块信息

| 字段   | 说明                  | 字段   | 说明                  |
| ------ | --------------------- | ------ | --------------------- |
| name   | 板块名称              | plate  | 板块类型              |
| month  | 最近1月涨幅(百分比)   | 6month | 最近6个月涨幅(百分比) |
| 3month | 最近3个月涨幅(百分比) | codes  | 包含该板块基金代码    |
| date   | 最新同步时间          | day    | 当日涨幅(百分比)      |
| week   | 最近1周涨幅(百分比)   | year   | 涨幅(百分比)          |

#### fund_info_coll 基金基本信息

| 字段   | 说明       | 字段 | 说明 |
| :----- | :---------- | ------ | ------ |
| code   | 基金代码   | bank | 托管银行 |
| compay         | 基金管理人       | dividend | 成立来分红(累计值：单位元) |
| dividend_count | 成立来分红次数   | found_date | 成立日期 |
| full_name      | 基金全称         | issue_date | 发行日期 |
| manager        | 基金经理人       | scale | 资产规模 |
| scale_date     | 资产规模截止时间 | share          | 最新份额规模(单位: 亿份)   |
| share_date | 最新份额规模时间 | short_name | 基金简称 |
| type           | 基金类型         | date | 最新净值时间 |
| net | 最新净值 | net_accumulate | 累计净值 |
| rise_1m | 最近1个月涨幅(百分比) | rise_3m | 最近3个月涨幅(百分比) |
| rise_6m | 最近6个月涨幅(百分比) | rise_1y | 最近1年涨幅(百分比) |
| rise_3y | 最近3年涨幅(百分比) | rise_found | 成立以来涨幅(百分比) |
| stock_annotate | 最新持仓股票，可读性较强格式 | stock_names | 最新持仓股票名称 |
| stock_codes | 最新持仓股票代码 | found_scale | 成立规模(单位: 亿份) |

### fund_net_db 基金净值数据库

净值数据库以基金代码作为集合名称。

| 字段           | 说明     | 字段          | 说明                     |
| -------------- | -------- | ------------- | ------------------------ |
| date           | 净值时间 | apply_status  | 申购状态                 |
| code           | 基金代码 | day_grow_rate | 日增长率(日涨幅，百分比) |
| dividend       | 分红配送 | net           | 净值                     |
| net_accumulate | 累计净值 | redeem_status | 赎回状态                 |
| short_name     | 基金简称 |               |                          |

