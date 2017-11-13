drop table if exists $(hivevar:targetDbName).temp_products;

create temporary table $(hivevar:targetDbName).temp_products(products_key STRING, load_dt TIMESTAMP, dtl__capxtimestamp TIMESTAMP,
dtl__capxaction STRING, dtl__capxrowid INT, edl_ingest_channel STRING, edl_ingest_time STRING, edl_soft_delete BOOLEAN
, edl_source_file STRING, product_code STRING, description STRING, standard_cost DECIMAL(19, 4), list_price DECIMAL(19, 4),
reorder_level INT, target_level INT, quantity_per_unit STRING, discontinued INT,
minimum_reorder_quantity INT, category STRING, attachments STRING);

insert overwrite table $(hivevar:targetDbName).temp_products
select 
    raw.products_key,
    raw.load_dt,
    raw.dtl__capxtimestamp,
    raw.dtl__capxaction,
    raw.dtl__capxrowid,
    raw.edl_ingest_channel,
    raw.edl_ingest_time,
    raw.edl_soft_delete,
    raw.edl_source_file,
    raw.product_code,
    raw.description,
    raw.standard_cost,
    raw.list_price,
    raw.reorder_level,
    raw.target_level,
    raw.quantity_per_unit,
    raw.discontinued,
    raw.minimum_reorder_quantity,
    raw.category,
    raw.attachments
from( select *
      from( select rank() over(partition by products_key order by dtl__capxtimestamp desc) as rnk, *
            from $(hivevar:sourceDbName).s_products) x
      where rnk = 1) raw
left join $(hivevar:targetDbName).s_products hub on raw.products_key = hub.products_key
where raw.load_dt > hub.load_dt or hub.load_dt is null;

delete from $(hivevar:targetDbName).s_products where exists ( select 1 from $(hivevar:targetDbName).temp_products temp where s_products.products_key = temp.products_key);

insert into table $(hivevar:targetDbName).s_products 
select
    products_key,
    load_dt,
    dtl__capxtimestamp,
    dtl__capxaction,
    dtl__capxrowid,
    edl_ingest_channel,
    edl_ingest_time,
    edl_soft_delete,
    edl_source_file,    
    product_code,
    description,
    standard_cost,
    list_price,
    reorder_level,
    target_level,
    quantity_per_unit,
    discontinued,
    minimum_reorder_quantity,
    category,
    attachments
from $(hivevar:targetDbName).temp_products where edl_soft_delete = 0;

drop table if exists $(hivevar:targetDbName).temp_products;