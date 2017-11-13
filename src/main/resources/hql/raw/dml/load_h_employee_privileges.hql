insert into table ${hivevar:targetDbName}.H_employee_privileges
select distinct 
        upper(concat_ws("-",regexp_replace(nvl(e.email_address,''), '"', ''), regexp_replace(nvl(p.privilege_name,''), '"', ''))) employee_privileges_key,
        upper(regexp_replace(nvl(e.email_address,''), '"', '')) employees_key,
        upper(regexp_replace(nvl(p.privilege_name,''), '"', '')) privileges_key,
        current_timestamp load_dt
from ${hivevar:sourceDbName}.stg_northwind_employee_privileges ep
join (select distinct id, email_address from ${hivevar:sourceDbName}.stg_northwind_employees) e on ep.employee_id = e.id
join (select distinct id, privilege_name from ${hivevar:sourceDbName}.stg_northwind_privileges) p on ep.privilege_id = p.id
where 1=1
and edl_ingest_time = ${hivevar:edlIngestTime}
and not exists (select 1 from ${hivevar:targetDbName}.h_employee_privileges hep where hep.employee_privileges_key = upper(concat_ws("-",regexp_replace(nvl(e.email_address,''), '"', ''), regexp_replace(nvl(p.privilege_name,''), '"', ''))));