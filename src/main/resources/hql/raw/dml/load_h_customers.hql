insert into table $(hivevar:targetDbName).H_customers
select distinct 
        upper(concat_ws("-",regexp_replace(last_name, '"', ''), regexp_replace(first_name, '"', ''), regexp_replace(company, '"', ''))) customers_key,
        id,
        regexp_replace(company, '"', '') company,
        current_timestamp 
from $(hivevar:sourceDbName).stg_northwind_customers sc
where not exists (select 1 from $(hivevar:targetDbName).h_customers hc where upper(concat_ws("-",regexp_replace(sc.last_name, '"', ''), regexp_replace(sc.first_name, '"', ''), regexp_replace(sc.company, '"', ''))) = hc.customers_key)
and edl_ingest_time = $(hivevar:edlIngestTime);