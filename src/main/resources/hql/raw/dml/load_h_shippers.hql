insert into table $(hivevar:targetDbName).h_shippers 
select distinct
        upper(concat_ws("-",regexp_replace(last_name, '"', ''), regexp_replace(first_name, '"', ''), regexp_replace(company, '"', ''))),
        id,
        regexp_replace(company, '"', ''),
        current_timestamp 
from $(hivevar:sourceDbName).stg_northwind_shippers ss
where not exists (select 1 from $(hivevar:targetDbName).h_shippers hs where upper(concat_ws("-",regexp_replace(ss.last_name, '"', ''), regexp_replace(ss.first_name, '"', ''), regexp_replace(ss.company, '"', ''))) = hs.shippers_key)
and edl_ingest_time = $(hivevar:edlIngestTime);