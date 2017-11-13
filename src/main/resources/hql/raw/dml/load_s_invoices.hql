insert into table ${hivevar:targetDbName}.s_invoices
select
        upper(cast(id as string)) invoices_key,
        current_timestamp load_dt,
        concat(substring(dtl__capxtimestamp,2,4),'-',substring(dtl__capxtimestamp,6,2),'-',substring(dtl__capxtimestamp,8,2),' ',substring(dtl__capxtimestamp,10,2),':',substring(dtl__capxtimestamp,12,2),':',substring(dtl__capxtimestamp,14,2),'.',substring(dtl__capxtimestamp,16,6)) dtl__capxtimestamp,
        regexp_replace(dtl__capxaction,'"','') dtl__capxaction,
        dtl__capxrowid dtl__capxrowid,
        null edl_soft_delete,         
        null edl_source_file,
        null edl_soft_delete,
        null edl_source_file,
        invoice_date,
        due_date,
        tax,
        shipping,
        amount_due
from ${hivevar:sourceDbName}.stg_northwind_invoices
where edl_ingest_time >= ${hivevar:edlIngestTime} and edl_ingest_channel = ${hivevar:edlIngestChannel};
