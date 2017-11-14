insert into table ${hivevar:targetDbName}.h_purchase_orders
select 
    purchase_orders_key,
    purchase_order_id,
    load_dt
from ${hivevar:sourceDbName}.h_purchase_orders raw
where not exists
    ( select 1
      from ${hivevar:targetDbName}.h_purchase_orders hub
      where raw.purchase_orders_key = hub.purchase_orders_key) ;