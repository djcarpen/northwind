insert into table ${hivevar:targetDbName}.r_purchase_order_status 
select distinct
        current_timestamp load_dt,
        concat(substring(dtl__capxtimestamp,2,4),'-',substring(dtl__capxtimestamp,6,2),'-',substring(dtl__capxtimestamp,8,2),' ',substring(dtl__capxtimestamp,10,2),':',substring(dtl__capxtimestamp,12,2),':',substring(dtl__capxtimestamp,14,2),'.',substring(dtl__capxtimestamp,16,6)) dtl__capxtimestamp,
        regexp_replace(dtl__capxaction,'"','') dtl__capxaction,
        dtl__capxrowid dtl__capxrowid,
        edl_ingest_channel,
        edl_ingest_time,
        null edl_soft_delete,
        null edl_source_file,
        id purchase_order_status_id,
        regexp_replace(status,'"','') status
from ${hivevar:sourceDbName}.stg_northwind_purchase_order_status spos
where not exists (select 1 from ${hivevar:targetDbName}.r_purchase_order_status rpos where spos.id = rpos.purchase_order_status_id)
and edl_ingest_time = ${hivevar:edlIngestTime};
