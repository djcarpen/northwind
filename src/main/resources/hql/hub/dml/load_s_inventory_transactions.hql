drop table if exists ${hivevar:targetDbName}.temp_inventory_transactions;

create temporary table ${hivevar:targetDbName}.temp_inventory_transactions(inventory_transactions_key STRING, load_dt
TIMESTAMP, dtl__capxtimestamp TIMESTAMP, dtl__capxaction STRING, dtl__capxrowid INT, edl_ingest_channel STRING,
edl_ingest_time STRING, edl_soft_delete BOOLEAN, edl_source_file STRING,transaction_type INT, transaction_created_date TIMESTAMP,
transaction_modified_date TIMESTAMP, quantity INT, comments STRING);

insert overwrite table ${hivevar:targetDbName}.temp_inventory_transactions
select 
    raw.inventory_transactions_key,
    raw.load_dt,
    raw.dtl__capxtimestamp,
    raw.dtl__capxaction,
    raw.dtl__capxrowid,
    raw.edl_ingest_channel,
    raw.edl_ingest_time,
    raw.edl_soft_delete,
    raw.edl_source_file,
    raw.transaction_type,
    raw.transaction_created_date,
    raw.transaction_modified_date,
    raw.quantity,
    raw.comments
from( select *
      from( select rank() over(partition by inventory_transactions_key order by dtl__capxtimestamp desc) as rnk, *
            from $(hivevar:sourceDbName).s_inventory_transactions) x
      where rnk = 1) raw
left join ${hivevar:targetDbName}.s_inventory_transactions hub on raw.inventory_transactions_key = hub.inventory_transactions_key
where raw.load_dt > hub.load_dt or hub.load_dt is null;

delete from ${hivevar:targetDbName}.s_inventory_transactions where exists ( select 1 from ${hivevar:targetDbName}.temp_inventory_transactions temp where s_inventory_transactions.inventory_transactions_key = temp.inventory_transactions_key);

insert into table ${hivevar:targetDbName}.s_inventory_transactions 
select
    inventory_transactions_key,
    load_dt,
    dtl__capxtimestamp,
    dtl__capxaction,
    dtl__capxrowid,
    edl_ingest_channel,
    edl_ingest_time,
    edl_soft_delete,
    edl_source_file,    
    transaction_type,
    transaction_created_date,
    transaction_modified_date,
    quantity,
    comments
from ${hivevar:targetDbName}.temp_inventory_transactions 
where edl_soft_delete = 0;

drop table if exists ${hivevar:targetDbName}.temp_inventory_transactions;