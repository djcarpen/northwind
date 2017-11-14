insert into table ${hivevar:targetDbName}.l_order_details
SELECT distinct             
    link_order_details_key,
    order_details_key,
    orders_key,
    products_key,
    purchase_orders_key,
    inventory_transactions_key,
    load_dt              
from $(hivevar:sourceDbName).l_order_details raw
where not exists ( select 1 
               from ${hivevar:targetDbName}.l_order_details hub
               where raw.link_order_details_key = hub.link_order_details_key);