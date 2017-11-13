insert into table ${hivevar:targetDbName}.h_inventory_transactions
select distinct 
    upper(cast(id as string)) inventory_transactions_key,
    CURRENT_TIMESTAMP() load_dt,
    null load_end_dt
from ${hivevar:sourceDbName}.stg_northwind_inventory_transactions sit
where not exists (select 1 from ${hivevar:targetDbName}.h_inventory_transactions hit where upper(cast(sit.id as string)) = hit.inventory_transactions_key)
and edl_ingest_time >= ${hivevar:edlIngestTime} and edl_ingest_channel = ${hivevar:edlIngestChannel};