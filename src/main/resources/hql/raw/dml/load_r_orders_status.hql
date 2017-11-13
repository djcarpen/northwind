insert into table ${hivevar:targetDbName}.r_orders_status 
select distinct
        current_timestamp load_dt,
        concat(substring(dtl__capxtimestamp,2,4),'-',substring(dtl__capxtimestamp,6,2),'-',substring(dtl__capxtimestamp,8,2),' ',substring(dtl__capxtimestamp,10,2),':',substring(dtl__capxtimestamp,12,2),':',substring(dtl__capxtimestamp,14,2),'.',substring(dtl__capxtimestamp,16,6)) dtl__capxtimestamp,
        regexp_replace(dtl__capxaction,'"','') dtl__capxaction,
        dtl__capxrowid dtl__capxrowid,
        edl_ingest_channel,
        edl_ingest_time,
        null edl_soft_delete,
        null edl_source_file,
        id order_status_id,
        regexp_replace(status_name,'"','') status_name
from ${hivevar:sourceDbName}.stg_northwind_orders_status sos
where not exists (select 1 from ${hivevar:targetDbName}.r_orders_status ros where sos.id = ros.order_status_id)
and edl_ingest_time = ${hivevar:edlIngestTime};
