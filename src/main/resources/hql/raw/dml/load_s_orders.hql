insert into table $(hivevar:targetDbName).s_orders
SELECT
        upper(cast(id as string)) orders_key,
        current_timestamp load_dt,
        concat(substring(dtl__capxtimestamp,2,4),'-',substring(dtl__capxtimestamp,6,2),'-',substring(dtl__capxtimestamp,8,2),' ',substring(dtl__capxtimestamp,10,2),':',substring(dtl__capxtimestamp,12,2),':',substring(dtl__capxtimestamp,14,2),'.',substring(dtl__capxtimestamp,16,6)) dtl__capxtimestamp,
        regexp_replace(dtl__capxaction,'"','') dtl__capxaction,
        dtl__capxrowid dtl__capxrowid,
        edl_ingest_channel,
        edl_ingest_time,
        null edl_soft_delete,         
        null edl_source_file,
        tax_status_id,
        status_id,
        order_date,
        shipped_date,
        trim(regexp_replace(ship_name,'"','')) ship_name,
        trim(regexp_replace(ship_address,'"','')) ship_address,
        trim(regexp_replace(ship_city,'"','')) ship_city,
        trim(regexp_replace(ship_state_province,'"','')) ship_state_province,
        trim(regexp_replace(ship_zip_postal_code,'"','')) ship_zip_postal_code,
        trim(regexp_replace(ship_country_region,'"','')) ship_country_region,
        trim(regexp_replace(shipping_fee,'"','')) shipping_fee,
        taxes,
        trim(regexp_replace(payment_type,'"','')) payment_type,
        paid_date,
        trim(regexp_replace(notes,'"','')) notes
        FROM $(hivevar:sourceDbName).stg_northwind_orders
where edl_ingest_time = $(hivevar:edlIngestTime);