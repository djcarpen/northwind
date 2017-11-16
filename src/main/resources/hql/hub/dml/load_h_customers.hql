insert into table ${hivevar:targetDbName}.h_customers
select customers_key,
    customer_id,
    company,
    load_dt from ${hivevar:sourceDbName}.h_customers raw where not exists (select 1 from ${hivevar:targetDbName}.h_customers hub where raw.customers_key=hub.customers_key);