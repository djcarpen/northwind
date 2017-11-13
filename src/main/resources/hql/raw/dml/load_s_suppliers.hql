insert into table ${hivevar:targetDbName}.s_suppliers
SELECT
        upper(concat_ws("-",cast(id as string), trim(regexp_replace(company,'"','')))) suppliers_key,
        current_timestamp load_dt,
        concat(substring(dtl__capxtimestamp,2,4),'-',substring(dtl__capxtimestamp,6,2),'-',substring(dtl__capxtimestamp,8,2),' ',substring(dtl__capxtimestamp,10,2),':',substring(dtl__capxtimestamp,12,2),':',substring(dtl__capxtimestamp,14,2),'.',substring(dtl__capxtimestamp,16,6)) dtl__capxtimestamp,
        regexp_replace(dtl__capxaction,'"','') dtl__capxaction,
        dtl__capxrowid dtl__capxrowid,
        edl_ingest_channel,
        edl_ingest_time,
        null edl_soft_delete,         
        null edl_source_file,
        trim(regexp_replace(last_name,'"','')) last_name,
        trim(regexp_replace(first_name,'"','')) first_name,
        trim(regexp_replace(email_address,'"','')) email_address,
        trim(regexp_replace(job_title,'"','')) job_title,
        trim(regexp_replace(business_phone,'"','')) business_phone,
        trim(regexp_replace(home_phone,'"','')) home_phone,
        trim(regexp_replace(mobile_phone,'"','')) mobile_phone,
        trim(regexp_replace(fax_number,'"','')) fax_number,
        trim(regexp_replace(address,'"','')) address,
        trim(regexp_replace(city,'"','')) city,
        trim(regexp_replace(state_province,'"','')) state_province,
        trim(regexp_replace(zip_postal_code,'"','')) zip_postal_code,
        trim(regexp_replace(country_region,'"','')) country_region,
        trim(regexp_replace(web_page,'"','')) web_page,
        trim(regexp_replace(notes,'"','')) notes,
        trim(regexp_replace(attachments,'"','')) attachments
FROM ${hivevar:sourceDbName}.stg_northwind_suppliers
where edl_ingest_time = ${hivevar:edlIngestTime};