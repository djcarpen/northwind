Question 1: Trend in sales over the last three years 


select  substr(order_date,0,7) mnth, sum(unit_price) amount
from stg_northwind_orders o
join stg_northwind_order_details od on od.order_id = o.id
where order_date is not null
group by substr(order_date,0,7);

select substr(order_date,0,7) mnth, sum(unit_price) amount
from s_order_details sod
join l_order_details lod on sod.order_details_key = lod.order_details_key
join s_orders so on lod.orders_key = so.orders_key
where order_date is not null
group by substr(order_date,0,7);


Question 2: Which employee performed most during the last three years? 
select * from stg_northwind_orders