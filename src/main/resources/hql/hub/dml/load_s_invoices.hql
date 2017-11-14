drop table if exists ${hivevar:targetDbName}.temp_invoices;

create temporary table ${hivevar:targetDbName}.temp_invoices(invoices_key STRING, load_dt TIMESTAMP, dtl__capxtimestamp TIMESTAMP,
dtl__capxaction STRING, dtl__capxrowid INT, edl_ingest_channel STRING, edl_ingest_time STRING, edl_soft_delete BOOLEAN,
edl_source_file STRING, invoice_date TIMESTAMP, due_date TIMESTAMP, tax DECIMAL(19, 4), shipping DECIMAL(19, 4),
amount_due DECIMAL(19, 4));

insert overwrite table ${hivevar:targetDbName}.temp_invoices
select 
    raw.invoices_key,
    raw.load_dt,
    raw.dtl__capxtimestamp,
    raw.dtl__capxaction,
    raw.dtl__capxrowid,
    raw.edl_ingest_channel,
    raw.edl_ingest_time,
    raw.edl_soft_delete,
    raw.edl_source_file,
    raw.invoice_date,
    raw.due_date,
    raw.tax,
    raw.shipping,
    raw.amount_due
from( select *
      from( select rank() over(partition by invoices_key order by dtl__capxtimestamp desc) as rnk, *
            from ${hivevar:sourceDbName}.s_invoices) x
      where rnk = 1) raw
left join ${hivevar:targetDbName}.s_invoices hub on raw.invoices_key = hub.invoices_key
where raw.load_dt > hub.load_dt or hub.load_dt is null;

delete from ${hivevar:targetDbName}.s_invoices where exists ( select 1 from ${hivevar:targetDbName}.temp_invoices temp where s_invoices.invoices_key = temp.invoices_key);

insert into table ${hivevar:targetDbName}.s_invoices 
select
    invoices_key,
    load_dt,
    dtl__capxtimestamp,
    dtl__capxaction,
    dtl__capxrowid,
    edl_ingest_channel,
    edl_ingest_time,
    edl_soft_delete,
    edl_source_file,    
    invoice_date,
    due_date,
    tax,
    shipping,
    amount_due
from ${hivevar:targetDbName}.temp_invoices 
where edl_soft_delete = 0;

drop table if exists ${hivevar:targetDbName}.temp_invoices;