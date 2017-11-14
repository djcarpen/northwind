insert overwrite table ${hivevar:targetDbName}.r_strings 
select load_dt, dtl__capxtimestamp, dtl__capxaction, dtl__capxrowid, edl_ingest_channel, edl_ingest_time, edl_soft_delete, edl_source_file, string_id, string_data 
from ( select rank() over(partition by string_id order by dtl__capxtimestamp desc) as rnk, *
       from ${hivevar:sourceDbName}.r_strings) x
where rnk = 1 and edl_soft_delete = 0;