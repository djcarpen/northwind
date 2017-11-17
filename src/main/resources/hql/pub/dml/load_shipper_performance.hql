insert overwrite table ${hivevar:targetDbName}.shipper_performance
select company, sum(delivery_count) delivery_count, sum(not_delivered) not_delivered, avg(shipping_fee) avg_freight_cost
from (
        select hs.company, case when shipped_date is not null then 1 else 0 end as delivery_count, case when shipped_date is null then 1 else 0 end as not_delivered, so.shipping_fee
        from ${hivevar:sourceDbName}.s_orders so 
        join ${hivevar:sourceDbName}.l_orders lo on so.orders_key = lo.orders_key
        join ${hivevar:sourceDbName}.h_shippers hs on lo.shippers_key = hs.shippers_key) x
group by company;