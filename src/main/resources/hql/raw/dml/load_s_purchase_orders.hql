insert into table $(hivevar:targetDbName).s_purchase_orders
SELECT
        upper(cast(id as string)) purchase_orders_key,
        current_timestamp load_dt,
        concat(substring(dtl__capxtimestamp,2,4),'-',substring(dtl__capxtimestamp,6,2),'-',substring(dtl__capxtimestamp,8,2),' ',substring(dtl__capxtimestamp,10,2),':',substring(dtl__capxtimestamp,12,2),':',substring(dtl__capxtimestamp,14,2),'.',substring(dtl__capxtimestamp,16,6)) dtl__capxtimestamp,
        regexp_replace(dtl__capxaction,'"','') dtl__capxaction,
        dtl__capxrowid dtl__capxrowid,
        edl_ingest_channel,
        edl_ingest_time,
        null edl_soft_delete,         
        null edl_source_file,
        status_id,
        submitted_date,
        creation_date,
        expected_date,
        shipping_fee,
        taxes,
        payment_date,
        payment_amount,
        trim(regexp_replace(payment_method,'"','')) payment_method,
        trim(regexp_replace(notes,'"','')) notes,
        approved_date
FROM $(hivevar:sourceDbName).stg_northwind_purchase_orders
where edl_ingest_time = $(hivevar:edlIngestTime);