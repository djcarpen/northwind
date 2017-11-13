insert into table ${hivevar:targetDbName}.l_purchase_orders 
select distinct
        upper(concat_ws("-",nvl(cast(po.id as string),''),nvl(cast(s.id as string),''), trim(regexp_replace(nvl(s.company,''),'"','')),regexp_replace(nvl(e1.email_address,''), '"', ''), regexp_replace(nvl(e2.email_address,''), '"', ''),regexp_replace(nvl(e3.email_address,''), '"', ''))) link_purchase_orders_key,
        upper(nvl(cast(po.id as string),'')) purchase_orders_key,
        upper(concat_ws("-",nvl(cast(s.id as string),''), trim(regexp_replace(nvl(s.company,''),'"','')))) suppliers_key,
        upper(regexp_replace(nvl(e1.email_address,''), '"', '')) created_by_employees_key,
        upper(regexp_replace(nvl(e2.email_address,''), '"', '')) approved_by_employees_key,
        upper(regexp_replace(nvl(e3.email_address,''), '"', '')) submitted_by_employees_key,
        CURRENT_TIMESTAMP() load_dt
from ${hivevar:sourceDbName}.stg_northwind_purchase_orders po
left join ${hivevar:sourceDbName}.stg_northwind_suppliers s on po.supplier_id = s.id
left join ${hivevar:sourceDbName}.stg_northwind_employees e1 on po.created_by = e1.id
left join ${hivevar:sourceDbName}.stg_northwind_employees e2 on po.approved_by = e2.id
left join ${hivevar:sourceDbName}.stg_northwind_employees e3 on po.submitted_by = e3.id
where po.edl_ingest_time >= ${hivevar:edlIngestTime} and edl_ingest_channel = ${hivevar:edlIngestChannel}
and not exists (select 1 from ${hivevar:targetDbName}.l_purchase_orders lpo where link_purchase_orders_key = upper(concat_ws("-",nvl(cast(po.id as string),''),nvl(cast(s.id as string),''), trim(regexp_replace(nvl(s.company,''),'"','')),regexp_replace(nvl(e1.email_address,''), '"', ''), regexp_replace(nvl(e2.email_address,''), '"', ''),regexp_replace(nvl(e3.email_address,''), '"', ''))));
 