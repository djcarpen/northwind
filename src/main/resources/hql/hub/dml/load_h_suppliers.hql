insert into table ${hivevar:targetDbName}.h_suppliers
select 
    suppliers_key,
    supplier_id,
    company,
    load_dt
from $(hivevar:sourceDbName).h_suppliers raw
where not exists
    ( select 1
      from ${hivevar:targetDbName}.h_suppliers hub
      where raw.suppliers_key = hub.suppliers_key) ;