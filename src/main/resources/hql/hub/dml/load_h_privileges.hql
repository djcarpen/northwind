insert into table ${hivevar:targetDbName}.h_privileges
select 
    privileges_key,
    privilege_id,
    privilege_name,
    load_dt
from ${hivevar:sourceDbName}.h_privileges raw
where not exists
    ( select 1
      from ${hivevar:targetDbName}.h_privileges hub
      where raw.privileges_key = hub.privileges_key) ;