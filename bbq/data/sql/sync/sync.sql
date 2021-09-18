-- :name test_connection :one
select current_date

-- fund
-- :name select_fund_codes :many
select code from fund_info

-- :name insert_fund_info :insert
insert into fund_info(code, name, type) values(:code, :name, :type)

-- :name select_fund_daily :one
select trade_date from fund_daily where code = :code order by trade_date desc limit 1

-- :name insert_fund_daily :insert
insert into fund_daily(code, trade_date, close, open, high, low, volume, turnover)
values(:code, :trade_date, :close, :open, :high, :low, :volume, :turnover)

-- :name select_fund_net :one
select trade_date from fund_net where code = :code  order by trade_date desc limit 1

-- :name insert_fund_net :insert
insert into fund_net(code, trade_date, net, net_acc, rise, apply_status, redeem_status)
values(:code, :trade_date, :net, :net_acc, :rise, :apply_status, :redeem_status)

-- stock
-- :name select_stock_codes :many
select code from stock_info

-- :name insert_stock_info :insert
insert into stock_info(code, name, listing_date, block) values(:code, :name, :listing_date, :block)
-- :name select_stock_daily :one
select trade_date from stock_daily where code = :code  order by trade_date desc limit 1

-- :name insert_stock_daily :insert
insert into stock_daily(code, trade_date, close, open, high, low, volume, turnover, hfq_factor)
values(:code, :trade_date, :close, :open, :high, :low, :volume, :turnover, :hfq_factor)

-- :name select_stock_index :one
select trade_date from stock_index where code = :code  order by trade_date desc limit 1

-- :name insert_stock_index :insert
insert into stock_index(code, trade_date, pe, pe_ttm, pb, ps, ps_ttm, dv_ratio, dv_ttm, total_mv)
values(:code, :trade_date, :pe, :pe_ttm, :pb, :ps, :ps_ttm, :dv_ratio, :dv_ttm, :total_mv)

-- :name select_stock_fq_factor :one
select trade_date from stock_fq_factor where code = :code  order by trade_date desc limit 1

-- :name delete_stock_fq_factor :affected
delete from stock_fq_factor where code = :code

-- :name insert_stock_fq_factor :insert
insert into stock_fq_factor(code, trade_date, hfq_factor, qfq_factor, sync_date)
values(:code, :trade_date, :hfq_factor, :qfq_factor, :sync_date)

-- :name select_index_info_codes :many
select code from stock_index_info

-- :name insert_stock_index_info :insert
insert into stock_index_info(code, name) values(:code, :name)

-- :name select_stock_index_daily :one
select trade_date from stock_index_daily where code = :code  order by trade_date desc limit 1

-- :name insert_stock_index_daily :insert
insert into stock_index_daily(code, trade_date, close, open, high, low, volume)
values(:code, :trade_date, :close, :open, :high, :low, :volume)

-- :name select_stock_ns_flow :one
select trade_date from stock_ns_flow order by trade_date desc limit 1

-- :name insert_stock_ns_flow :insert
insert into stock_ns_flow(trade_date, sz_north_value, sh_north_value, north_value, sz_south_value, sh_south_value, south_value)
values(:trade_date, :sz_north_value, :sh_north_value, :north_value, :sz_south_value, :sh_south_value, :south_value)

-- :name select_stock_his_divend :one
select sync_date from stock_his_divend order by sync_date desc limit 1

-- :name delete_stock_his_divend :affected
delete from stock_his_divend where 1=1

-- :name insert_stock_his_divend :insert
insert into stock_his_divend(code, name, listing_date, divend_acc, divend_avg, divend_count, financed_total, financed_count, sync_date)
values(:code, :name, :listing_date, :divend_acc, :divend_avg, :divend_count, :financed_total, :financed_count, :sync_date)

-- :name select_stock_sw_index_info_codes :many
select distinct index_code from stock_sw_index_info

-- :name insert_stock_sw_index_info :insert
insert into stock_sw_index_info(index_code, index_name, stock_code, stock_name, start_date, weight)
values(:index_code, :index_name, :stock_code, :stock_name, :start_date, :weight)


