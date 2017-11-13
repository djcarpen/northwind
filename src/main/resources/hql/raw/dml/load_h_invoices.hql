insert into table ${hivevar:targetDbName}.h_invoices
select
        upper(cast(si.id as string)) invoices_key,
        si.id invoice_id,
        CURRENT_TIMESTAMP() load_dt
from ${hivevar:sourceDbName}.stg_northwind_invoices si
where not exists (select 1 from ${hivevar:targetDbName}.h_invoices hi where upper(cast(si.id as string)) = hi.invoices_key)
and edl_ingest_time = ${hivevar:edlIngestTime};