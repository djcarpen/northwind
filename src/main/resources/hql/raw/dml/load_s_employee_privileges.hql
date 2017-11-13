insert into ${hivevar:targetDbName}.s_employee_privileges 
select distinct
        upper(concat_ws("-",regexp_replace(nvl(e.email_address,''), '"', ''), regexp_replace(nvl(p.privilege_name,''), '"', ''))) employee_privileges_key,
        current_timestamp load_dt,
        concat(substring(dtl__capxtimestamp,2,4),'-',substring(dtl__capxtimestamp,6,2),'-',substring(dtl__capxtimestamp,8,2),' ',substring(dtl__capxtimestamp,10,2),':',substring(dtl__capxtimestamp,12,2),':',substring(dtl__capxtimestamp,14,2),'.',substring(dtl__capxtimestamp,16,6)) dtl__capxtimestamp,
        regexp_replace(dtl__capxaction,'"','') dtl__capxaction,
        dtl__capxrowid dtl__capxrowid,
        edl_ingest_channel,
        edl_ingest_time,
        null edl_soft_delete,         
        null edl_source_file
from ${hivevar:sourceDbName}.stg_northwind_employee_privileges ep
join (select distinct id, email_address from ${hivevar:sourceDbName}.stg_northwind_employees) e on ep.employee_id = e.id
join (select distinct id, privilege_name from ${hivevar:sourceDbName}.stg_northwind_privileges) p on ep.privilege_id = p.id
where 1=1
and edl_ingest_time >= ${hivevar:edlIngestTime} and edl_ingest_channel = ${hivevar:edlIngestChannel}
and not exists (select 1 from ${hivevar:targetDbName}.s_employee_privileges sep where sep.employee_privileges_key = upper(concat_ws("-",regexp_replace(nvl(e.email_address,''), '"', ''), regexp_replace(nvl(p.privilege_name,''), '"', ''))));
   