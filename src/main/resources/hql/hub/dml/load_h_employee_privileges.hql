insert into table ${hivevar:targetDbName}.h_employee_privileges
select
    employee_privileges_key,
    employees_key,
    privileges_key,
    load_dt
from $(hivevar:sourceDbName).h_employee_privileges raw
where not exists
    ( select 1
      from ${hivevar:targetDbName}.h_employee_privileges hub
      where raw.employee_privileges_key = hub.employee_privileges_key) ;