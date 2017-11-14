drop table if exists ${hivevar:targetDbName}.temp_purchase_order_details;

create temporary table ${hivevar:targetDbName}.temp_purchase_order_details(purchase_order_details_key STRING, load_dt
TIMESTAMP, dtl__capxtimestamp TIMESTAMP, dtl__capxaction STRING, dtl__capxrowid INT, edl_ingest_channel STRING,
edl_ingest_time STRING, edl_soft_delete BOOLEAN, edl_source_file STRING, quantity DECIMAL(18, 4), unit_cost DECIMAL(19, 4),
date_received TIMESTAMP, posted_to_inventory INT);

insert overwrite table ${hivevar:targetDbName}.temp_purchase_order_details
select 
    raw.purchase_order_details_key,
    raw.load_dt,
    raw.dtl__capxtimestamp,
    raw.dtl__capxaction,
    raw.dtl__capxrowid,
    raw.edl_ingest_channel,
    raw.edl_ingest_time,
    raw.edl_soft_delete,
    raw.edl_source_file,
    raw.quantity,
    raw.unit_cost,
    raw.date_received,
    raw.posted_to_inventory
from( select *
      from( select rank() over(partition by purchase_order_details_key order by load_dt desc) as rnk, *
            from ${hivevar:sourceDbName}.s_purchase_order_details) x
      where rnk = 1) raw
left join ${hivevar:targetDbName}.s_purchase_order_details hub on raw.purchase_order_details_key = hub.purchase_order_details_key
where raw.load_dt > hub.load_dt or hub.load_dt is null;

delete from ${hivevar:targetDbName}.s_purchase_order_details where exists ( select 1 from ${hivevar:targetDbName}.temp_purchase_order_details temp where s_purchase_order_details.purchase_order_details_key = temp.purchase_order_details_key);

insert into table ${hivevar:targetDbName}.s_purchase_order_details 
select
    purchase_order_details_key,
    load_dt,
    dtl__capxtimestamp,
    dtl__capxaction,
    dtl__capxrowid,
    edl_ingest_channel,
    edl_ingest_time,
    edl_soft_delete,
    edl_source_file,    
    quantity,
    unit_cost,
    date_received,
    posted_to_inventory
from ${hivevar:targetDbName}.temp_purchase_order_details 
where edl_soft_delete = 0;

drop table if exists ${hivevar:targetDbName}.temp_purchase_order_details;