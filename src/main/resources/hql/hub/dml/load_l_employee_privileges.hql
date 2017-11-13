insert into table $(hivevar:targetDbName).l_employee_privileges
SELECT distinct
    link_employee_privileges_key,
    employee_privileges_key,
    employees_key,
    privileges_key,
    load_dt
FROM $(hivevar:sourceDbName).l_employee_privileges raw
where not exists
    ( select 1
      from $(hivevar:targetDbName).l_employee_privileges hub
      where raw.link_employee_privileges_key = hub.link_employee_privileges_key) ;