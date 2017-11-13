insert into table $(hivevar:targetDbName).h_orders 
select distinct
        cast(so.id as string) orders_key,
        so.id order_id,
        CURRENT_TIMESTAMP() load_dt
from $(hivevar:sourceDbName).stg_northwind_orders so
where not exists (select 1 from $(hivevar:targetDbName).h_orders ho where cast(so.id as string) = ho.orders_key)
and edl_ingest_time = $(hivevar:edlIngestTime);