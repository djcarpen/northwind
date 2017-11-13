insert into table ${hivevar:targetDbName}.l_order_details  
select distinct
        upper(concat_ws("-",nvl(cast(od.id as string),''),nvl(cast(od.order_id as string),''),regexp_replace(nvl(p.product_name,''), '"', ''),nvl(cast(od.purchase_order_id as string),''),nvl(cast(od.inventory_id as string),''))) link_order_details_key,
        upper(nvl(cast(od.id as string),'')) order_details_key,
        upper(nvl(cast(od.order_id as string),'')) orders_key,
        upper(regexp_replace(nvl(p.product_name,''), '"', '')) products_key,
        upper(nvl(cast(od.purchase_order_id as string),'')) purchase_orders_key,
        upper(nvl(cast(od.inventory_id as string),'')) inventory_transactions_key,
        CURRENT_TIMESTAMP() load_dt
from ${hivevar:sourceDbName}.stg_northwind_order_details od
left join ${hivevar:sourceDbName}.stg_northwind_products p on od.product_id = p.id
where od.edl_ingest_time >= ${hivevar:edlIngestTime} and edl_ingest_channel = ${hivevar:edlIngestChannel}
and not exists (select 1 from ${hivevar:targetDbName}.l_order_details lod where lod.link_order_details_key = upper(concat_ws("-",nvl(cast(od.id as string),''),nvl(cast(od.order_id as string),''),regexp_replace(nvl(p.product_name,''), '"', ''),nvl(cast(od.purchase_order_id as string),''),nvl(cast(od.inventory_id as string),''))));
