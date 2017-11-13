insert into table $(hivevar:targetDbName).h_privileges
select distinct
        upper(regexp_replace(privilege_name, '"', '')),
        id,
        regexp_replace(privilege_name, '"', ''),
        current_timestamp 
from $(hivevar:sourceDbName).stg_northwind_privileges sp
where not exists (select 1 from $(hivevar:targetDbName).h_privileges hp where upper(regexp_replace(sp.privilege_name, '"', '')) = hp.privileges_key)
and edl_ingest_time = $(hivevar:edlIngestTime);