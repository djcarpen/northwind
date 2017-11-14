drop table if exists ${hivevar:targetDbName}.temp_orders;

create temporary table ${hivevar:targetDbName}.temp_orders(orders_key STRING, load_dt TIMESTAMP, dtl__capxtimestamp TIMESTAMP, dtl__capxaction
STRING, dtl__capxrowid INT, edl_ingest_channel STRING, edl_ingest_time STRING, edl_soft_delete BOOLEAN,
edl_source_file STRING, tax_status_id INT, status_id INT, order_date TIMESTAMP, shipped_date TIMESTAMP, ship_name STRING,
ship_address STRING, ship_city STRING, ship_state_province STRING, ship_zip_postal_code STRING,
ship_country_region STRING, shipping_fee DECIMAL(19, 4), taxes DECIMAL(19, 4), payment_type STRING,
paid_date TIMESTAMP, notes STRING);

insert overwrite table ${hivevar:targetDbName}.temp_orders
select 
    raw.orders_key,
    raw.load_dt,
    raw.dtl__capxtimestamp,
    raw.dtl__capxaction,
    raw.dtl__capxrowid,
    raw.edl_ingest_channel,
    raw.edl_ingest_time,
    raw.edl_soft_delete,
    raw.edl_source_file,
    raw.tax_status_id,
    raw.status_id,
    raw.order_date,
    raw.shipped_date,
    raw.ship_name,
    raw.ship_address,
    raw.ship_city,
    raw.ship_state_province,
    raw.ship_zip_postal_code,
    raw.ship_country_region,
    raw.shipping_fee,
    raw.taxes,
    raw.payment_type,
    raw.paid_date,
    raw.notes
from( select *
      from( select rank() over(partition by orders_key order by dtl__capxtimestamp desc) as rnk, *
            from $(hivevar:sourceDbName).s_orders) x
      where rnk = 1) raw
left join ${hivevar:targetDbName}.s_orders hub on raw.orders_key = hub.orders_key
where raw.load_dt > hub.load_dt or hub.load_dt is null;

delete from ${hivevar:targetDbName}.s_orders where exists ( select 1 from ${hivevar:targetDbName}.temp_orders temp where s_orders.orders_key = temp.orders_key);

insert into table ${hivevar:targetDbName}.s_orders 
select
    orders_key,
    load_dt,
    dtl__capxtimestamp,
    dtl__capxaction,
    dtl__capxrowid,
    edl_ingest_channel,
    edl_ingest_time,
    edl_soft_delete,
    edl_source_file,    
    tax_status_id,
    status_id,
    order_date,
    shipped_date,
    ship_name,
    ship_address,
    ship_city,
    ship_state_province,
    ship_zip_postal_code,
    ship_country_region,
    shipping_fee,
    taxes,
    payment_type,
    paid_date,
    notes
from ${hivevar:targetDbName}.temp_orders 
where edl_soft_delete = 0;

drop table if exists ${hivevar:targetDbName}.temp_orders;