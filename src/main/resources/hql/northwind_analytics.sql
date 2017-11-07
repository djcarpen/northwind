



-- Question 1: Trend in sales over the last six months 


select  substr(order_date,0,4) `year`, substr(order_date,6,2) `month`, sum(unit_price) amount
from stg_northwind_orders o
join stg_northwind_order_details od on od.order_id = o.id and cast(substr(order_date,0,4) as int) = 2006 and cast(substr(order_date,6,2) as int) between 1 and 6 
group by substr(order_date,0,4), substr(order_date,6,2)
order by `year`,`month`;

select substr(order_date,0,4) `year`, substr(order_date,6,2) `month`, sum(unit_price) amount
from s_order_details sod
join l_order_details lod on sod.order_details_key = lod.order_details_key
join s_orders so on lod.orders_key = so.orders_key and cast(substr(order_date,0,4) as int) = 2006 and cast(substr(order_date,6,2) as int) between 1 and 6 
group by substr(order_date,0,4), substr(order_date,6,2)
order by `year`,`month`;


-- Question 2: Which employee performed most during the last six months? 

select concat_ws(' ',e.first_name, e.last_name) employee_name, e.job_title, sum(unit_price) amount
from stg_northwind_orders o
join stg_northwind_order_details od on o.id = od.order_id and cast(substr(order_date,0,4) as int) = 2006 and cast(substr(order_date,6,2) as int) between 1 and 6
join stg_northwind_employees e on o.employee_id = e.id
group by e.first_name, e.last_name, e.job_title
order by amount desc;

select concat_ws(' ',se.first_name, se.last_name) employee_name, se.job_title, sum(unit_price) amount
from s_order_details sod
join l_order_details lod on sod.order_details_key = lod.order_details_key
join s_orders so on lod.orders_key = so.orders_key and cast(substr(order_date,0,4) as int) = 2006 and cast(substr(order_date,6,2) as int) between 1 and 6 
join l_orders lo on so.orders_key = lo.orders_key
join s_employees se on lo.employees_key = se.employees_key
group by se.first_name, se.last_name, se.job_title
order by amount desc;

--Question 3: Sales by State, Customer & Product 
-- Sales by Geography (state, customer count, quantity, total sales

select ship_state_province `state`, count(distinct customer_id) `customer count`, sum(quantity) quantity, sum(unit_price) amount
from stg_northwind_orders o
join stg_northwind_order_details od on od.order_id = o.id and o.order_date is not null
group by ship_state_province
order by amount desc;

select ship_state_province `state`, count(distinct lo.customers_key) `customer count`, sum(quantity) quantity, sum(unit_price) amount
from s_order_details sod
join l_order_details lod on sod.order_details_key = lod.order_details_key
join s_orders so on lod.orders_key = so.orders_key and so.order_date is not null
join l_orders lo on so.orders_key = lo.orders_key
group by ship_state_province
order by amount desc;

-- Sales by Customer (customer name, product name, order count, quantity, total sales)

select c.company, product_name, count(o.id) order_count, sum(quantity) quantity, sum(unit_price) amount
from stg_northwind_orders o
join stg_northwind_order_details od on od.order_id = o.id and o.order_date is not null
join stg_northwind_products p on od.product_id = p.id
join stg_northwind_customers c on o.customer_id = c.id
group by c.company, product_name
order by amount desc;

select hc.company, product_name, count(so.orders_key) order_count, sum(sod.quantity) quantity, sum(sod.unit_price) amount
from s_order_details sod
join l_order_details lod on sod.order_details_key = lod.order_details_key
join s_orders so on lod.orders_key = so.orders_key and so.order_date is not null
join l_orders lo on so.orders_key = lo.orders_key
join h_customers hc on lo.customers_key = hc.customers_key
join h_products hp on lod.products_key = hp.products_key
group by hc.company, product_name
order by amount desc;



-- Sales by Product (product name, order count, quantity, total sales)
select product_name, count(o.id) order_count, sum(quantity) quantity, sum(unit_price) amount
from stg_northwind_orders o
join stg_northwind_order_details od on od.order_id = o.id and o.order_date is not null
join stg_northwind_products p on od.product_id = p.id
group by product_name
order by amount desc;


select product_name, count(so.orders_key) order_count, sum(sod.quantity) quantity, sum(sod.unit_price) amount
from s_order_details sod
join l_order_details lod on sod.order_details_key = lod.order_details_key
join s_orders so on lod.orders_key = so.orders_key and so.order_date is not null
join l_orders lo on so.orders_key = lo.orders_key
join h_products hp on lod.products_key = hp.products_key
group by product_name
order by amount desc;

-- Question 4: Shipper performance (shipper name, delivery count, not delivered,  average freight cost

select company, sum(delivery_count) delivery_count, sum(not_delivered) not_delivered, avg(shipping_fee) avg_freight_cost
from (
        select s.company, case when shipped_date is not null then 1 else 0 end as delivery_count, case when shipped_date is null then 1 else 0 end as not_delivered, o.shipping_fee
        from stg_northwind_orders o 
        join stg_northwind_shippers s on o.shipper_id = s.id) x
group by company;


select company, sum(delivery_count) delivery_count, sum(not_delivered) not_delivered, avg(shipping_fee) avg_freight_cost
from (
        select hs.company, case when shipped_date is not null then 1 else 0 end as delivery_count, case when shipped_date is null then 1 else 0 end as not_delivered, so.shipping_fee
        from s_orders so 
        join l_orders lo on so.orders_key = lo.orders_key
        join h_shippers hs on lo.shippers_key = hs.shippers_key) x
group by company;


-- Question 5: Supplier Performance (supplier company name, product name, item count, total quantity)

select split(split(regexp_replace(supplier_ids,'"',''),','),';')
from stg_northwind_products p


