insert into table ${hivevar:targetDbName}.h_shippers
select 
    shippers_key,
    shipper_id,
    company,
    load_dt
from ${hivevar:sourceDbName}.h_shippers raw
where not exists (select 1 from ${hivevar:targetDbName}.h_shippers hub where raw.shippers_key = hub.shippers_key);