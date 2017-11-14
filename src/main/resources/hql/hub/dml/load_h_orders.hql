insert into table ${hivevar:targetDbName}.h_orders
select 
    orders_key,
    order_id,
    load_dt
from ${hivevar:sourceDbName}.h_orders raw
where not exists
    ( select 1
      from ${hivevar:targetDbName}.h_orders hub
      where raw.orders_key = hub.orders_key) ;