insert overwrite table $(hivevar:targetDbName).r_sales_reports 
select load_dt, dtl__capxtimestamp, dtl__capxaction, dtl__capxrowid, edl_ingest_channel, edl_ingest_time, edl_soft_delete, edl_source_file, group_by, display, title, filter_row_source, default 
from ( select rank() over(partition by group_by, display, title, filter_row_source, default order by dtl__capxtimestamp desc) as rnk, *
       from $(hivevar:sourceDbName).r_sales_reports) x
where rnk = 1;