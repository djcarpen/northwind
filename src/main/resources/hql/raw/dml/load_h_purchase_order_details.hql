insert into table ${hivevar:targetDbName}.h_purchase_order_details
select distinct 
    upper(cast(spod.id as string)) purchase_order_details_key,
    spod.id purchase_order_details_id,
    CURRENT_TIMESTAMP() load_dt
FROM ${hivevar:sourceDbName}.stg_northwind_purchase_order_details spod
where not exists (select 1 from ${hivevar:targetDbName}.h_purchase_order_details hpod where upper(cast(spod.id as string)) = hpod.purchase_order_details_key)
and edl_ingest_time = ${hivevar:edlIngestTime};