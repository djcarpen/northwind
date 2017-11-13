drop table if exists $(hivevar:targetDbName).temp_purchase_orders;

create temporary table $(hivevar:targetDbName).temp_purchase_orders(purchase_orders_key STRING, load_dt TIMESTAMP, dtl__capxtimestamp
TIMESTAMP, dtl__capxaction STRING, dtl__capxrowid INT, edl_ingest_channel STRING, edl_ingest_time STRING,
edl_soft_delete BOOLEAN, edl_source_file STRING, submitted_date TIMESTAMP, creation_date TIMESTAMP, status_id INT, expected_date
TIMESTAMP, shipping_fee DECIMAL(19, 4), taxes DECIMAL(19, 4), payment_date TIMESTAMP,
payment_amount DECIMAL(19, 4), payment_method STRING, notes STRING, approved_date TIMESTAMP);

insert overwrite table $(hivevar:targetDbName).temp_purchase_orders
select 
    raw.purchase_orders_key,
    raw.load_dt,
    raw.dtl__capxtimestamp,
    raw.dtl__capxaction,
    raw.dtl__capxrowid,
    raw.edl_ingest_channel,
    raw.edl_ingest_time,
    raw.edl_soft_delete,
    raw.edl_source_file,
    raw.submitted_date,
    raw.creation_date,
    raw.status_id,
    raw.expected_date,
    raw.shipping_fee,
    raw.taxes,
    raw.payment_date,
    raw.payment_amount,
    raw.payment_method,
    raw.notes,
    raw.approved_date
from( select *
      from( select rank() over(partition by purchase_orders_key order by load_dt desc) as rnk, *
            from $(hivevar:sourceDbName).s_purchase_orders) x
      where rnk = 1) raw
left join $(hivevar:targetDbName).s_purchase_orders hub on raw.purchase_orders_key = hub.purchase_orders_key
where raw.load_dt > hub.load_dt or hub.load_dt is null;

delete from $(hivevar:targetDbName).s_purchase_orders where exists ( select 1 from $(hivevar:targetDbName).temp_purchase_orders temp where s_purchase_orders.purchase_orders_key = temp.purchase_orders_key);

insert into table $(hivevar:targetDbName).s_purchase_orders 
select
    purchase_orders_key,
    load_dt,
    dtl__capxtimestamp,
    dtl__capxaction,
    dtl__capxrowid,
    edl_ingest_channel,
    edl_ingest_time,
    edl_soft_delete,
    edl_source_file,    
    submitted_date,
    creation_date,
    status_id,
    expected_date,
    shipping_fee,
    taxes,
    payment_date,
    payment_amount,
    payment_method,
    notes,
    approved_date
from $(hivevar:targetDbName).temp_purchase_orders 
where edl_soft_delete = 0;

drop table if exists $(hivevar:targetDbName).temp_purchase_orders;