insert into table ${hivevar:targetDbName}.h_products
select 
    products_key,
    product_id,
    product_name,
    load_dt
from $(hivevar:sourceDbName).h_products raw
where not exists
    ( select 1
      from ${hivevar:targetDbName}.h_products hub
      where raw.products_key = hub.products_key) ;