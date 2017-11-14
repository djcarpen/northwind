insert into table ${hivevar:targetDbName}.h_order_details
select 
    order_details_key,
    order_detail_id,
    load_dt
from ${hivevar:sourceDbName}.h_order_details raw
where not exists
    ( select 1
      from ${hivevar:targetDbName}.h_order_details hub
      where raw.order_details_key = hub.order_details_key) ;