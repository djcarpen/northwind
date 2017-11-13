drop table if exists $(hivevar:targetDbName).temp_employee_privileges;

create temporary table $(hivevar:targetDbName).temp_employee_privileges(employee_privileges_key STRING, load_dt TIMESTAMP, dtl__capxtimestamp TIMESTAMP, 
dtl__capxaction STRING, dtl__capxrowid INT, edl_ingest_channel STRING, edl_ingest_time STRING, edl_soft_delete BOOLEAN, edl_source_file STRING) ;

insert overwrite table $(hivevar:targetDbName).temp_employee_privileges
select distinct 
    raw.employee_privileges_key,
    raw.load_dt,
    raw.dtl__capxtimestamp,
    raw.dtl__capxaction,
    raw.dtl__capxrowid,
    raw.edl_ingest_channel,
    raw.edl_ingest_time,
    raw.edl_soft_delete,         
    raw.edl_source_file
from( select *
      from( select rank() over(partition by employee_privileges_key order by load_dt desc) as rnk, *
            from $(hivevar:sourceDbName).s_employee_privileges) x
      where rnk = 1) raw
left join $(hivevar:targetDbName).s_employee_privileges hub on raw.employee_privileges_key = hub.employee_privileges_key
where raw.load_dt > hub.load_dt or hub.load_dt is null;


delete from $(hivevar:targetDbName).s_employee_privileges where exists ( select 1 from $(hivevar:targetDbName).temp_employee_privileges temp where s_employee_privileges.employee_privileges_key = temp.employee_privileges_key);

insert into table $(hivevar:targetDbName).s_employee_privileges
select 
    employee_privileges_key,
    load_dt,
    dtl__capxtimestamp,
    dtl__capxaction,
    dtl__capxrowid,
    edl_ingest_channel,
    edl_ingest_time,
    edl_soft_delete,         
    edl_source_file
from $(hivevar:targetDbName).temp_employee_privileges
where edl_soft_delete = 0;

drop table if exists $(hivevar:targetDbName).temp_employee_privileges;