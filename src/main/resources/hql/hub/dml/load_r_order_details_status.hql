insert overwrite table $(hivevar:targetDbName).r_order_details_status 
select load_dt, dtl__capxtimestamp, dtl__capxaction, dtl__capxrowid, edl_ingest_channel, edl_ingest_time, edl_soft_delete, edl_source_file, order_details_status_id, status_name 
from ( select rank() over(partition by order_details_status_id order by dtl__capxtimestamp desc) as rnk, *
       from $(hivevar:sourceDbName).r_order_details_status) x
where rnk = 1 and edl_soft_delete = 0;