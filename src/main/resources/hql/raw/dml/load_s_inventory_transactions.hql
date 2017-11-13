insert into table ${hivevar:targetDbName}.s_inventory_transactions
select 
        upper(cast(id as string)) inventory_transactions_key,
        current_timestamp load_dt,
        concat(substring(dtl__capxtimestamp,2,4),'-',substring(dtl__capxtimestamp,6,2),'-',substring(dtl__capxtimestamp,8,2),' ',substring(dtl__capxtimestamp,10,2),':',substring(dtl__capxtimestamp,12,2),':',substring(dtl__capxtimestamp,14,2),'.',substring(dtl__capxtimestamp,16,6)) dtl__capxtimestamp,
        regexp_replace(dtl__capxaction,'"','') dtl__capxaction,
        dtl__capxrowid dtl__capxrowid,
        edl_ingest_channel,
        edl_ingest_time,
        null edl_soft_delete,         
        null edl_source_file,
        transaction_type,
        transaction_created_date,
        transaction_modified_date,
        quantity,
        trim(regexp_replace(comments,'"','')) comments
from ${hivevar:sourceDbName}.stg_northwind_inventory_transactions
where edl_ingest_time >= ${hivevar:edlIngestTime} and edl_ingest_channel = ${hivevar:edlIngestChannel};
 