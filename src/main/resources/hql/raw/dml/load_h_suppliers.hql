insert into table ${hivevar:targetDbName}.h_suppliers 
select distinct
        upper(concat_ws("-",cast(id as string), trim(regexp_replace(company,'"','')))) suppliers_key,
        id supplier_id,
        trim(regexp_replace(company,'"','')),
        CURRENT_TIMESTAMP() load_dt
from ${hivevar:sourceDbName}.stg_northwind_suppliers ss
where not exists (select 1 from ${hivevar:targetDbName}.h_suppliers hs where upper(concat_ws("-",cast(ss.id as string), trim(regexp_replace(ss.company,'"','')))) = hs.suppliers_key)
and edl_ingest_time >= ${hivevar:edlIngestTime} and edl_ingest_channel = ${hivevar:edlIngestChannel};