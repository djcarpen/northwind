insert into table ${hivevar:targetDbName}.l_purchase_order_details
SELECT distinct               
    link_purchase_order_details_key,
    purchase_order_details_key,
    purchase_orders_key,
    inventory_transactions_key,
    products_key,
    load_dt        
FROM $(hivevar:sourceDbName).l_purchase_order_details raw
where not exists ( select 1 
               from ${hivevar:targetDbName}.l_purchase_order_details hub
               where raw.link_purchase_order_details_key = hub.link_purchase_order_details_key);