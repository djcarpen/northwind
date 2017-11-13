insert into table ${hivevar:targetDbName}.l_orders
select distinct
        upper(concat_ws("-",nvl(cast(o.id as string),''),regexp_replace(nvl(e.email_address,''), '"', ''),regexp_replace(nvl(c.last_name,''), '"', ''), regexp_replace(nvl(c.first_name,''), '"', ''), regexp_replace(nvl(c.company,''), '"', ''),regexp_replace(nvl(sh.last_name,''), '"', ''), regexp_replace(nvl(sh.first_name,''), '"', ''), regexp_replace(nvl(sh.company,''), '"', ''))) link_orders_key,
        upper(nvl(cast(o.id as string),'')) orders_key,
        upper(regexp_replace(nvl(e.email_address,''), '"', '')) employees_key,
        upper(concat_ws("-",regexp_replace(nvl(c.last_name,''), '"', ''), regexp_replace(nvl(c.first_name,''), '"', ''), regexp_replace(nvl(c.company,''), '"', ''))) customers_key,
        upper(concat_ws("-",regexp_replace(nvl(sh.last_name,''), '"', ''), regexp_replace(nvl(sh.first_name,''), '"', ''), regexp_replace(nvl(sh.company,''), '"', ''))) shippers_key,
        CURRENT_TIMESTAMP() load_dt
from ${hivevar:sourceDbName}.stg_northwind_orders o
left join ${hivevar:sourceDbName}.stg_northwind_employees e on o.employee_id = e.id
left join ${hivevar:sourceDbName}.stg_northwind_customers c on o.customer_id = c.id
left join ${hivevar:sourceDbName}.stg_northwind_shippers sh on o.shipper_id = sh.id
where o.edl_ingest_time >= ${hivevar:edlIngestTime} and edl_ingest_channel = ${hivevar:edlIngestChannel}
and not exists (select 1 from ${hivevar:targetDbName}.l_orders where link_orders_key = upper(concat_ws("-",nvl(cast(o.id as string),''),regexp_replace(nvl(e.email_address,''), '"', ''),regexp_replace(nvl(c.last_name,''), '"', ''), regexp_replace(nvl(c.first_name,''), '"', ''), regexp_replace(nvl(c.company,''), '"', ''),regexp_replace(nvl(sh.last_name,''), '"', ''), regexp_replace(nvl(sh.first_name,''), '"', ''), regexp_replace(nvl(sh.company,''), '"', ''))));
