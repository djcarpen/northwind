insert into table ${hivevar:targetDbName}.l_invoices
select distinct
        upper(concat_ws("-",regexp_replace(nvl(cast(i.id as string),''), '"', ''), regexp_replace(nvl(cast(i.order_id as string),''), '"', ''))) link_invoices_key,
        upper(nvl(cast(i.id as string),'')) invoices_key,
        upper(nvl(cast(i.order_id as string),'')) orders_key,
        CURRENT_TIMESTAMP() load_dt
from ${hivevar:sourceDbName}.stg_northwind_invoices i
where edl_ingest_time = ${hivevar:edlIngestTime}
and not exists (select 1 from ${hivevar:targetDbName}.l_invoices lo where link_invoices_key = upper(concat_ws("-",regexp_replace(nvl(cast(i.id as string),''), '"', ''), regexp_replace(nvl(cast(i.order_id as string),''), '"', ''))));    
