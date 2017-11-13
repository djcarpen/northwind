insert into table $(hivevar:targetDbName).r_sales_reports 
select 
        current_timestamp load_dt,
        concat(substring(dtl__capxtimestamp,2,4),'-',substring(dtl__capxtimestamp,6,2),'-',substring(dtl__capxtimestamp,8,2),' ',substring(dtl__capxtimestamp,10,2),':',substring(dtl__capxtimestamp,12,2),':',substring(dtl__capxtimestamp,14,2),'.',substring(dtl__capxtimestamp,16,6)) dtl__capxtimestamp,
        regexp_replace(dtl__capxaction,'"','') dtl__capxaction,
        dtl__capxrowid dtl__capxrowid,
        edl_ingest_channel,
        edl_ingest_time,
        null edl_soft_delete,
        null edl_source_file,
        regexp_replace(group_by,'"','') group_by,
        regexp_replace(display,'"','') display,
        regexp_replace(title,'"','') title,
        regexp_replace(filter_row_source,'"','') filter_row_source,
        `default`
from $(hivevar:sourceDbName).stg_northwind_sales_reports ssr
where not exists (
        select 1 from $(hivevar:targetDbName).r_sales_reports rsr 
        where regexp_replace(ssr.group_by,'"','') = rsr.group_by
                and regexp_replace(ssr.display,'"','') = rsr.display
                and regexp_replace(ssr.title,'"','') = rsr.title
                and regexp_replace(ssr.filter_row_source,'"','') = rsr.filter_row_source
                and ssr.`default` = rsr.`default`)
and edl_ingest_time = $(hivevar:edlIngestTime);