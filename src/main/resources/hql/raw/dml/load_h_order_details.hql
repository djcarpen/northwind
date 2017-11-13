insert into table $(hivevar:targetDbName).h_order_details 
select 
    upper(cast(sod.id as string)) order_details_key,
    sod.id order_details_id,
    CURRENT_TIMESTAMP() load_dt
FROM $(hivevar:sourceDbName).stg_northwind_order_details sod
where not exists (select 1 from $(hivevar:targetDbName).h_order_details hod where upper(cast(sod.id as string)) = hod.order_details_key)
and edl_ingest_time = $(hivevar:edlIngestTime);