insert overwrite table ${hivevar:targetDbName}.sales_trend_by_month
select year(order_date) `year`, month(order_date) `month`, sum(unit_price) amount
from ${hivevar:sourceDbName}.s_order_details sod
join ${hivevar:sourceDbName}.l_order_details lod on sod.order_details_key = lod.order_details_key
join ${hivevar:sourceDbName}.s_orders so on lod.orders_key = so.orders_key and cast(substr(order_date,0,4) as int) = 2006 and cast(substr(order_date,6,2) as int) between 1 and 6 
group by year(order_date), month(order_date);