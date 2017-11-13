insert into table ${hivevar:targetDbName}.s_products
select 
        upper(regexp_replace(product_name, '"', '')),
        current_timestamp load_dt,
        concat(substring(dtl__capxtimestamp,2,4),'-',substring(dtl__capxtimestamp,6,2),'-',substring(dtl__capxtimestamp,8,2),' ',substring(dtl__capxtimestamp,10,2),':',substring(dtl__capxtimestamp,12,2),':',substring(dtl__capxtimestamp,14,2),'.',substring(dtl__capxtimestamp,16,6)) dtl__capxtimestamp,
        regexp_replace(dtl__capxaction,'"','') dtl__capxaction,
        dtl__capxrowid dtl__capxrowid,
        edl_ingest_channel,
        edl_ingest_time,
        null edl_soft_delete,         
        null edl_source_file,
        regexp_replace(product_code, '"', ''),
        regexp_replace(description, '"', ''),
        standard_cost,
        list_price,
        reorder_level,
        target_level,
        regexp_replace(quantity_per_unit, '"', ''),
        discontinued,
        minimum_reorder_quantity,
        regexp_replace(category, '"', ''),
        regexp_replace(attachments, '"', '') 
from ${hivevar:sourceDbName}.stg_northwind_products
where edl_ingest_time = ${hivevar:edlIngestTime};