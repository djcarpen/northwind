
-- Question 1: Trend in sales over the last six months 


select  substr(order_date,0,4) `year`, substr(order_date,6,2) `month`, sum(unit_price) amount
from stg_northwind_orders o
join stg_northwind_order_details od on od.order_id = o.id and cast(substr(order_date,0,4) as int) = 2006 and cast(substr(order_date,6,2) as int) between 1 and 6 
group by substr(order_date,0,4), substr(order_date,6,2)
order by `year`,`month`;

select year(order_date) `year`, month(order_date) `month`, sum(unit_price) amount
from (select * from (select rank() over (partition by order_details_key order by mod_dt desc) as rnk, order_details_key, quantity, unit_price from s_order_details) x where rnk = 1) sod
join l_order_details lod on sod.order_details_key = lod.order_details_key
join (select * from (select rank() over (partition by orders_key order by mod_dt desc) as rnk, orders_key, order_date from s_orders) x where rnk = 1) so on lod.orders_key = so.orders_key and cast(substr(order_date,0,4) as int) = 2006 and cast(substr(order_date,6,2) as int) between 1 and 6 
group by year(order_date), month(order_date)
order by `year`,`month`;


-- Question 2: Which employee performed most during the last six months? 

select concat_ws(' ',e.first_name, e.last_name) employee_name, e.job_title, sum(unit_price) amount
from stg_northwind_orders o
join stg_northwind_order_details od on o.id = od.order_id and cast(substr(order_date,0,4) as int) = 2006 and cast(substr(order_date,6,2) as int) between 1 and 6
join stg_northwind_employees e on o.employee_id = e.id
group by e.first_name, e.last_name, e.job_title
order by amount desc;

select concat_ws(' ',se.first_name, se.last_name) employee_name, se.job_title, sum(unit_price) amount
from (select * from (select rank() over (partition by order_details_key order by mod_dt desc) as rnk, order_details_key, quantity, unit_price from s_order_details) x where rnk = 1) sod
join l_order_details lod on sod.order_details_key = lod.order_details_key
join (select * from (select rank() over (partition by orders_key order by mod_dt desc) as rnk, orders_key, order_date, ship_state_province from s_orders) x where rnk = 1) so on lod.orders_key = so.orders_key and cast(substr(order_date,0,4) as int) = 2006 and cast(substr(order_date,6,2) as int) between 1 and 6 
join l_orders lo on so.orders_key = lo.orders_key
join (select * from (select rank() over (partition by employees_key order by mod_dt desc) as rnk, employees_key, first_name, last_name, job_title from s_employees) x where rnk = 1) se on lo.employees_key = se.employees_key
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
from (select * from (select rank() over (partition by order_details_key order by mod_dt desc) as rnk, order_details_key, quantity, unit_price from s_order_details) x where rnk = 1) sod
join l_order_details lod on sod.order_details_key = lod.order_details_key
join (select * from (select rank() over (partition by orders_key order by mod_dt desc) as rnk, orders_key, order_date, ship_state_province from s_orders) x where rnk = 1) so on lod.orders_key = so.orders_key and so.order_date is not null
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
from (select * from (select rank() over (partition by order_details_key order by mod_dt desc) as rnk, order_details_key, quantity, unit_price from s_order_details) x where rnk = 1) sod
join l_order_details lod on sod.order_details_key = lod.order_details_key
join (select * from (select rank() over (partition by orders_key order by mod_dt desc) as rnk, orders_key, order_date from s_orders) x where rnk = 1) so on lod.orders_key = so.orders_key and so.order_date is not null
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
from (select * from (select rank() over (partition by order_details_key order by mod_dt desc) as rnk, order_details_key, quantity, unit_price from s_order_details) x where rnk = 1)  sod
join l_order_details lod on sod.order_details_key = lod.order_details_key
join (select * from (select rank() over (partition by orders_key order by mod_dt desc) as rnk, orders_key, order_date from s_orders) x where rnk = 1) so on lod.orders_key = so.orders_key and so.order_date is not null
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
        from (select * from (select rank() over (partition by orders_key order by mod_dt desc) as rnk, orders_key, shipped_date, shipping_fee from s_orders) x where rnk = 1) so 
        join l_orders lo on so.orders_key = lo.orders_key
        join h_shippers hs on lo.shippers_key = hs.shippers_key) x
group by company;


-- Question 5: Supplier Performance (supplier company name, product name, item count)

select s.company, p.product_name, sum(pod.quantity) as item_count
from stg_northwind_purchase_orders po
join stg_northwind_purchase_order_details pod on po.id = pod.purchase_order_id
join stg_northwind_suppliers s on po.supplier_id = s.id
join stg_northwind_products p on pod.product_id = p.id
group by s.company, p.product_name
order by company, product_name;

select hs.company, hp.product_name, sum(spod.quantity) as item_count
from (select * from (select rank() over (partition by purchase_orders_key order by mod_dt desc) as rnk, purchase_orders_key from s_purchase_orders) x where rnk = 1) spo
join l_purchase_order_details lpod on lpod.purchase_orders_key = spo.purchase_orders_key
join (select * from (select rank() over (partition by purchase_order_details_key order by mod_dt desc) as rnk, purchase_order_details_key, quantity from s_purchase_order_details) x where rnk = 1) spod on lpod.purchase_order_details_key = spod.purchase_order_details_key
join l_purchase_orders lpo on spo.purchase_orders_key = lpo.purchase_orders_key
join h_suppliers hs on lpo.suppliers_key = hs.suppliers_key
join h_products hp on hp.products_key = lpod.products_key
group by hs.company, hp.product_name
order by company, product_name;


