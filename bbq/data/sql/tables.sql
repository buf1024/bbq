-- fund
-- 'code': '基金代码', 'name': '基金简称', 'type': '基金类型'
create table fund_info
(
    code char(6)     not null primary key,
    name varchar(32) not null,
    type varchar(16) not null
);

-- 'code': '基金代码', 'trade_date': '交易日'， 'net': '净值', 'net_acc': '累计净值',
-- 'rise': '日增长率', 'apply_status': '申购状态', 'redeem_status': '赎回状态'
create table fund_net
(
    id            integer        not null primary key auto_increment,
    code          char(6)        not null,
    trade_date    datetime       not null,
    net           decimal(16, 4) not null,
    net_acc       decimal(16, 4) not null,
    rise          decimal(16, 4) not null,
    apply_status  varchar(16) null,
    redeem_status varchar(16) null
);
create
index fund_code_idx on fund_net(code, trade_date);

-- 'code': '代码', 'trade_date': '交易日', 'close': '收盘价', 'open': '开盘价', 'high': '最高价', 'low': '最低价',
-- 'volume': '成交量(股)', 'turnover': '换手率'
create table fund_daily
(
    id         integer        not null primary key auto_increment,
    code       char(6)        not null,
    trade_date datetime       not null,
    close      decimal(16, 4) not null,
    open       decimal(16, 4) not null,
    high       decimal(16, 4) not null,
    low        decimal(16, 4) not null,
    volume     decimal(16, 4) not null,
    turnover   decimal(16, 4) not null
);
create
index fund_daily_idx on fund_daily(code, trade_date);

-- stock
-- 'code': '代码', 'name': '名称', 'listing_date': '上市日期', 'block': '板块'
create table stock_info
(
    code char(8)     not null primary key,
    name varchar(32) not null,
    listing_date datetime not null,
    block varchar(16) not null
);

-- 'code': '代码', 'trade_date': '交易日', 'close': '收盘价', 'open': '开盘价', 'high': '最高价', 'low': '最低价',
-- 'volume': '成交量(股)', 'turnover': '换手率', 'hfq_factor': '后复权因子'
create table stock_daily
(
    id         integer        not null primary key auto_increment,
    code       char(8)        not null,
    trade_date datetime       not null,
    close      decimal(16, 4) not null,
    open       decimal(16, 4) not null,
    high       decimal(16, 4) not null,
    low        decimal(16, 4) not null,
    volume     decimal(16, 4) not null,
    turnover   decimal(16, 4) not null,
    hfq_factor decimal(16, 4) not null
);
create
index stock_daily_idx on stock_daily(code, trade_date);

--  'code': '代码', 'name': '名称', 'trade_date': '交易日',
--  'spj': '收盘价(元)', 'zdf': '涨跌幅(%)',
--  'rzye': '融资余额(元)(RZYE)', 'rzyezb': '融资余额占流通市值比(%)(RZYEZB)', 'rzmre': '融资买入额(元)',
--  'rzche': '融资偿还额(元)', 'rzjme': '融资净买入(元)',
--  'rqye': '融券余额(元)', 'rqyl': '融券余量(股)', 'rqmcl': '融券卖出量(股)', 'rqchl': '融券偿还量(股)',
--  'rqjmg': '净卖出(股)',
--  'rzrqye': '融资融券余额(元)', 'rzrqyecz': '融资融券余额差值(元)'
create table stock_margin
(
    id          integer        not null primary key auto_increment,
    code        char(8)        not null,
    name        varchar(32)    null,
    trade_date  datetime       not null,
    spj         decimal(16, 4) not null,
    zdf         decimal(16, 4) not null,
    rzye        decimal(16, 4) not null,
    rzyezb      decimal(16, 4) not null,
    rzmre       decimal(16, 4) not null,
    rzche       decimal(16, 4) not null,
    rzjme       decimal(16, 4) not null,
    rqye        decimal(16, 4) not null,
    rqyl        decimal(16, 4) not null,
    rqmcl       decimal(16, 4) not null,
    rqchl       decimal(16, 4) not null,
    rqjmg       decimal(16, 4) not null,
    rzrqye      decimal(16, 4) not null,
    rzrqyecz    decimal(16, 4) not null
);
create
index stock_margin_idx on stock_margin(code, trade_date);

-- 'code': '代码', 'trade_date': '交易日', 'pe': '市盈率', 'pe_ttm': '市盈率TTM',
-- 'pb': '市净率', 'ps': '市销率', 'ps_ttm': '市销率TTM', 'dv_ratio': '股息率', 'dv_ttm': '股息率TTM',
-- 'total_mv': '总市值'
create table stock_index
(
    id         integer        not null primary key auto_increment,
    code       char(8)        not null,
    trade_date datetime       not null,
    pe         decimal(16, 4) not null,
    pe_ttm     decimal(16, 4) not null,
    pb         decimal(16, 4) not null,
    ps         decimal(16, 4) not null,
    ps_ttm     decimal(16, 4) not null,
    dv_ratio   decimal(16, 4) not null,
    dv_ttm     decimal(16, 4) not null,
    total_mv   decimal(16, 4) not null
);
create
index stock_index_idx on stock_index(code, trade_date);
--         'stock_fq_factor': {'code': '代码', 'trade_date': '交易日', 'hfq_factor': '后复权因子', 'qfq_factor': '前复权因子',
--                             'sync_date': '最近同步时间(避免全量同步程序使用)'},
create table stock_fq_factor
(
    id         integer        not null primary key auto_increment,
    code       char(8)        not null,
    trade_date datetime       not null,
    hfq_factor decimal(16, 4) not null,
    qfq_factor decimal(16, 4) not null,
    sync_date  datetime       not null
);
create
index stock_fq_factor_idx on stock_fq_factor(code, trade_date);

-- 'code': '代码', 'name': '名称'
create table stock_index_info
(
    code char(8)     not null primary key,
    name varchar(32) not null
);
-- 'code': '代码', 'trade_date': '交易日', 'close': '收盘价', 'open': '开盘价', 'high': '最高价', 'low': '最低价',
-- 'volume': '成交量(股)'
create table stock_index_daily
(
    id         integer        not null primary key auto_increment,
    code       char(8)        not null,
    trade_date datetime       not null,
    close      decimal(16, 4) not null,
    open       decimal(16, 4) not null,
    high       decimal(16, 4) not null,
    low        decimal(16, 4) not null,
    volume     decimal(16, 4) not null
);
create
index stock_index_daily_idx on stock_index_daily(code, trade_date);
-- 'trade_date': '交易日',
-- 'sz_north_value': '深股通北上', 'sh_north_value': '沪股通北上', 'north_value': '北上资金',
-- 'sz_south_value': '深股通南下', 'sh_south_value': '沪股通南下', 'south_value': '南下资金'
create table stock_ns_flow
(
    id             integer        not null primary key auto_increment,
    trade_date     datetime       not null,
    sz_north_value decimal(16, 4) not null,
    sh_north_value decimal(16, 4) not null,
    north_value    decimal(16, 4) not null,
    sz_south_value decimal(16, 4) not null,
    sh_south_value decimal(16, 4) not null,
    south_value    decimal(16, 4) not null
);
create
index stock_stock_ns_flow_idx on stock_ns_flow(trade_date);

-- 'code': '代码', 'name': '名称', 'listing_date': '上市日期', 'divend_acc': '累计股息',
-- 'divend_avg': '年均股息', 'divend_count': '分红次数', 'financed_total': '融资总额',
-- 'financed_count': '融资次数'

create table stock_his_divend
(
    id             integer        not null primary key auto_increment,
    code           char(8)        not null,
    name           varchar(32)    not null,
    listing_date   datetime       not null,
    divend_acc     decimal(16, 4) not null,
    divend_avg     decimal(16, 4) not null,
    divend_count   integer        not null,
    financed_total decimal(16, 4) not null,
    financed_count integer        not null,
    sync_date      datetime
);
create
index stock_his_divend_idx on stock_his_divend(code);

-- 'index_code': '行业代码', 'index_name': '行业名称', 'stock_code': '股票代码', 'stock_name': '股票名称',
-- 'start_date': '开始日期', 'weight': '权重'
create table stock_sw_index_info
(
    id         integer        not null primary key auto_increment,
    index_code char(6)        not null,
    index_name varchar(32)    not null,
    stock_code char(8)        not null,
    stock_name varchar(32)    not null,
    start_date datetime       not null,
    weight     decimal(16, 4) not null
);
create
index stock_sw_index_info_idx on stock_sw_index_info(index_code, stock_code);
