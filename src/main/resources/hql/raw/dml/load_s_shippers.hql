insert into table ${hivevar:targetDbName}.s_shippers
select
        upper(concat_ws("-",regexp_replace(last_name, '"', ''), regexp_replace(first_name, '"', ''), regexp_replace(company, '"', ''))),
        current_timestamp load_dt,
        concat(substring(dtl__capxtimestamp,2,4),'-',substring(dtl__capxtimestamp,6,2),'-',substring(dtl__capxtimestamp,8,2),' ',substring(dtl__capxtimestamp,10,2),':',substring(dtl__capxtimestamp,12,2),':',substring(dtl__capxtimestamp,14,2),'.',substring(dtl__capxtimestamp,16,6)) dtl__capxtimestamp,
        regexp_replace(dtl__capxaction,'"','') dtl__capxaction,
        dtl__capxrowid dtl__capxrowid,
        edl_ingest_channel,
        edl_ingest_time,
        null edl_soft_delete,         
        null edl_source_file,
        regexp_replace(last_name, '"', ''),
        regexp_replace(first_name, '"', ''),
        regexp_replace(email_address, '"', ''),
        regexp_replace(job_title, '"', ''),
        regexp_replace(business_phone, '"', ''),
        regexp_replace(home_phone, '"', ''),
        regexp_replace(mobile_phone, '"', ''),
        regexp_replace(fax_number, '"', ''),
        regexp_replace(address, '"', ''),
        regexp_replace(city, '"', ''),
        regexp_replace(state_province, '"', ''),
        regexp_replace(zip_postal_code, '"', ''),
        regexp_replace(country_region, '"', ''),
        regexp_replace(web_page, '"', ''),
        regexp_replace(notes, '"', ''),
        regexp_replace(attachments, '"', '') 
from ${hivevar:sourceDbName}.stg_northwind_shippers
where edl_ingest_time = ${hivevar:edlIngestTime};