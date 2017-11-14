insert overwrite table ${hivevar:targetDbName}.r_inventory_transaction_types
select load_dt, dtl__capxtimestamp, dtl__capxaction, dtl__capxrowid, edl_ingest_channel, edl_ingest_time, edl_soft_delete, edl_source_file, inventory_transaction_type_id, type_name 
from ( select rank() over(partition by inventory_transaction_type_id order by dtl__capxtimestamp desc) as rnk, *
       from $(hivevar:sourceDbName).r_inventory_transaction_types) x
where rnk = 1 and edl_soft_delete = 0;