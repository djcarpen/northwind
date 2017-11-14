insert into table ${hivevar:targetDbName}.l_purchase_order_details
select distinct
        upper(concat_ws("-",nvl(cast(pod.id as string),''),nvl(cast(pod.purchase_order_id as string),''),nvl(cast(pod.inventory_id as string),''),nvl(regexp_replace(p.product_name, '"', ''),''))) link_purchase_order_details_key,
        upper(nvl(cast(pod.id as string),'')) purchase_order_details_key,
        upper(nvl(cast(pod.purchase_order_id as string),'')) purchase_orders_key,
        upper(nvl(cast(pod.inventory_id as string),'')) inventory_transactions_key,
        upper(nvl(regexp_replace(p.product_name, '"', ''),'')) products_key,
        CURRENT_TIMESTAMP() load_dt
from ${hivevar:sourceDbName}.stg_northwind_purchase_order_details pod
left join ${hivevar:sourceDbName}.stg_northwind_products p on pod.product_id = p.id
where pod.edl_ingest_time >= ${hivevar:edlIngestTime} and pod.edl_ingest_channel = ${hivevar:edlIngestChannel}
and not exists (select 1 from ${hivevar:targetDbName}.l_purchase_order_details where link_purchase_order_details_key = upper(concat_ws("-",nvl(cast(pod.id as string),''),nvl(cast(pod.purchase_order_id as string),''),nvl(cast(pod.inventory_id as string),''),nvl(regexp_replace(p.product_name, '"', ''),''))));
 