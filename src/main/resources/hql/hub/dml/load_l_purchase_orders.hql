insert into table $(hivevar:targetDbName).l_purchase_orders
SELECT distinct               
    link_purchase_orders_key,
    purchase_orders_key,
    suppliers_key,
    created_by_employees_key,
    approved_by_employees_key,
    submitted_by_employees_key,
    load_dt     
FROM $(hivevar:sourceDbName).l_purchase_orders raw
where not exists ( select 1 
               from $(hivevar:targetDbName).l_purchase_orders hub
               where raw.link_purchase_orders_key = hub.link_purchase_orders_key);