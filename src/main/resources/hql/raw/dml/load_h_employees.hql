insert into table ${hivevar:targetDbName}.h_employees
select distinct
        upper(regexp_replace(email_address, '"', '')),
        id,
        regexp_replace(email_address, '"', ''),
        current_timestamp 
from ${hivevar:sourceDbName}.stg_northwind_employees se
where not exists (select 1 from ${hivevar:targetDbName}.h_employees he where upper(regexp_replace(se.email_address, '"', '')) = he.employees_key)
and edl_ingest_time = ${hivevar:edlIngestTime};