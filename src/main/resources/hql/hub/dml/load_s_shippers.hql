drop table if exists $(hivevar:targetDbName).temp_shippers;

create temporary table $(hivevar:targetDbName).temp_shippers(shippers_key STRING, load_dt TIMESTAMP, dtl__capxtimestamp TIMESTAMP,
dtl__capxaction STRING, dtl__capxrowid INT, edl_ingest_channel STRING, edl_ingest_time STRING, edl_soft_delete BOOLEAN
, edl_source_file STRING, last_name STRING, first_name STRING, email_address STRING, job_title STRING, business_phone
STRING, home_phone STRING, mobile_phone STRING, fax_number STRING, address STRING, city STRING,
state_province STRING, zip_postal_code STRING, country_region STRING, web_page STRING, notes STRING
, attachments STRING);

insert overwrite table $(hivevar:targetDbName).temp_shippers
select 
    raw.shippers_key,
    raw.load_dt,
    raw.dtl__capxtimestamp,
    raw.dtl__capxaction,
    raw.dtl__capxrowid,
    raw.edl_ingest_channel,
    raw.edl_ingest_time,
    raw.edl_soft_delete,
    raw.edl_source_file,
    raw.last_name,
    raw.first_name,
    raw.email_address,
    raw.job_title,
    raw.business_phone,
    raw.home_phone,
    raw.mobile_phone,
    raw.fax_number,
    raw.address,
    raw.city,
    raw.state_province,
    raw.zip_postal_code,
    raw.country_region,
    raw.web_page,
    raw.notes,
    raw.attachments
from( select *
      from( select rank() over(partition by shippers_key order by load_dt desc) as rnk, *
            from $(hivevar:sourceDbName).s_shippers) x
      where rnk = 1) raw
left join $(hivevar:targetDbName).s_shippers hub on raw.shippers_key = hub.shippers_key
where raw.load_dt > hub.load_dt or hub.load_dt is null;

delete from $(hivevar:targetDbName).s_shippers where exists ( select 1 from $(hivevar:targetDbName).temp_shippers temp where s_shippers.shippers_key = temp.shippers_key);

insert into table $(hivevar:targetDbName).s_shippers 
select
    shippers_key,
    load_dt,
    dtl__capxtimestamp,
    dtl__capxaction,
    dtl__capxrowid,
    edl_ingest_channel,
    edl_ingest_time,
    edl_soft_delete,
    edl_source_file,    
    last_name,
    first_name,
    email_address,
    job_title,
    business_phone,
    home_phone,
    mobile_phone,
    fax_number,
    address,
    city,
    state_province,
    zip_postal_code,
    country_region,
    web_page,
    notes,
    attachments
from $(hivevar:targetDbName).temp_shippers 
where edl_soft_delete = 0;

drop table if exists $(hivevar:targetDbName).temp_shippers;