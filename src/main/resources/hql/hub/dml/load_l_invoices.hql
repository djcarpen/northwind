insert into table ${hivevar:targetDbName}.l_invoices
SELECT distinct
    link_invoices_key,
    invoices_key,
    orders_key,
    load_dt  
FROM $(hivevar:sourceDbName).l_invoices raw
where not exists ( select 1 
               from ${hivevar:targetDbName}.l_invoices hub
               where raw.link_invoices_key = hub.link_invoices_key);