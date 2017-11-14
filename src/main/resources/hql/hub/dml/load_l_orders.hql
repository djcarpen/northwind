insert into table ${hivevar:targetDbName}.l_orders
SELECT distinct             
    link_orders_key,
    orders_key,
    employees_key,
    customers_key,
    shippers_key,
    load_dt             
FROM ${hivevar:sourceDbName}.l_orders raw
where not exists ( select 1 
               from ${hivevar:targetDbName}.l_orders hub
               where raw.link_orders_key = hub.link_orders_key);