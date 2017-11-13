insert into table ${hivevar:targetDbName}.h_products 
select distinct
        upper(regexp_replace(sp.product_name, '"', '')),
        sp.id,
        regexp_replace(sp.product_name, '"', ''),
        current_timestamp 
from ${hivevar:sourceDbName}.stg_northwind_products sp
where not exists (select 1 from ${hivevar:targetDbName}.h_products hp where upper(regexp_replace(sp.product_name, '"', '')) = hp.products_key)
and edl_ingest_time >= ${hivevar:edlIngestTime} and edl_ingest_channel = ${hivevar:edlIngestChannel};