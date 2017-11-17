insert overwrite table ${hivevar:targetDbName}.employee_performance_by_month
select concat_ws(' ',se.first_name, se.last_name) employee_name, se.job_title, sum(unit_price) amount
from ${hivevar:sourceDbName}.s_order_details sod
join ${hivevar:sourceDbName}.l_order_details lod on sod.order_details_key = lod.order_details_key
join ${hivevar:sourceDbName}.s_orders so on lod.orders_key = so.orders_key and cast(substr(order_date,0,4) as int) = 2006 and cast(substr(order_date,6,2) as int) between 1 and 6 
join ${hivevar:sourceDbName}.l_orders lo on so.orders_key = lo.orders_key
join ${hivevar:sourceDbName}.s_employees se on lo.employees_key = se.employees_key
group by se.first_name, se.last_name, se.job_title;