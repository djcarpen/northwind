insert into table ${hivevar:targetDbName}.h_purchase_orders
select distinct
    upper(cast(id as string)) purchase_orders_key,
    spo.id purchase_order_id,
    CURRENT_TIMESTAMP() load_dt
FROM ${hivevar:sourceDbName}.stg_northwind_purchase_orders spo
where not exists (select 1 from ${hivevar:targetDbName}.h_purchase_orders hpo where upper(cast(spo.id as string)) = hpo.purchase_orders_key)
and edl_ingest_time >= ${hivevar:edlIngestTime} and edl_ingest_channel = ${hivevar:edlIngestChannel};