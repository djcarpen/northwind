insert into table ${hivevar:targetDbName}.h_purchase_order_details
select 
    purchase_order_details_key,
    purchase_order_detail_id,
    load_dt
from ${hivevar:sourceDbName}.h_purchase_order_details raw
where not exists (select 1 from ${hivevar:targetDbName}.h_purchase_order_details hub where raw.purchase_order_details_key = hub.purchase_order_details_key) ;