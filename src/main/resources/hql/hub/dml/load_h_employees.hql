insert into table $(hivevar:targetDbName).h_employees 
select 
    employees_key,
    employee_id,
    email_address,
    load_dt
from $(hivevar:sourceDbName).h_employees raw
where not exists
    ( select 1
       from $(hivevar:targetDbName).h_employees hub
       where raw.employees_key = hub.employees_key) ;