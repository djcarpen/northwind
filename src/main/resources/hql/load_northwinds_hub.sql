drop table if exists temp_customers;
drop table if exists temp_employees;
drop table if exists temp_inventory_transactions;
drop table if exists temp_invoices;
drop table if exists temp_order_details;
drop table if exists temp_orders;
drop table if exists temp_products;
drop table if exists temp_purchase_order_details;
drop table if exists temp_purchase_orders;
drop table if exists temp_shippers;
drop table if exists temp_suppliers;
drop table if exists temp_l_invoices;
drop table if exists temp_l_order_details;
drop table if exists temp_l_orders;
drop table if exists temp_l_purchase_order_details;
drop table if exists temp_l_purchase_orders;
drop table if exists temp_lsat_employee_privileges;

create temporary table temp_customers(customers_key STRING, load_dt TIMESTAMP, mod_dt TIMESTAMP,
mod_type STRING, mod_row_id INT, edl_ingest_channel STRING, edl_ingest_time STRING, deleted BOOLEAN
, last_name STRING, first_name STRING, email_address STRING, job_title STRING, business_phone
STRING, home_phone STRING, mobile_phone STRING, fax_number STRING, address STRING, city STRING,
state_province STRING, zip_postal_code STRING, country_region STRING, web_page STRING, notes STRING
, attachments STRING) ;

create temporary table temp_employees(employees_key STRING, load_dt TIMESTAMP, mod_dt TIMESTAMP,
mod_type STRING, mod_row_id INT, edl_ingest_channel STRING, edl_ingest_time STRING, deleted BOOLEAN
, company STRING, last_name STRING, first_name STRING, job_title STRING, business_phone STRING,
home_phone STRING, mobile_phone STRING, fax_number STRING, address STRING, city STRING,
state_province STRING, zip_postal_code STRING, country_region STRING, web_page STRING, notes STRING
, attachments STRING) ;

create temporary table temp_inventory_transactions(inventory_transactions_key STRING, load_dt
TIMESTAMP, mod_dt TIMESTAMP, mod_type STRING, mod_row_id INT, edl_ingest_channel STRING,
edl_ingest_time STRING, deleted BOOLEAN, transaction_type INT, transaction_created_date TIMESTAMP,
transaction_modified_date TIMESTAMP, quantity INT, comments STRING) ;

create temporary table temp_invoices(invoices_key STRING, load_dt TIMESTAMP, mod_dt TIMESTAMP,
mod_type STRING, mod_row_id INT, edl_ingest_channel STRING, edl_ingest_time STRING, deleted BOOLEAN
, invoice_date TIMESTAMP, due_date TIMESTAMP, tax DECIMAL(19, 4), shipping DECIMAL(19, 4),
amount_due DECIMAL(19, 4)) ;

create temporary table temp_order_details(order_details_key STRING, load_dt TIMESTAMP, mod_dt
TIMESTAMP, mod_type STRING, mod_row_id INT, edl_ingest_channel STRING, edl_ingest_time STRING,
deleted BOOLEAN, quantity DECIMAL(18, 4), unit_price DECIMAL(19, 4), discount DOUBLE, status_id INT
, date_allocated TIMESTAMP) ;

create temporary table temp_orders(orders_key STRING, load_dt TIMESTAMP, mod_dt TIMESTAMP, mod_type
STRING, mod_row_id INT, edl_ingest_channel STRING, edl_ingest_time STRING, deleted BOOLEAN,
tax_status_id INT, status_id INT, order_date TIMESTAMP, shipped_date TIMESTAMP, ship_name STRING,
ship_address STRING, ship_city STRING, ship_state_province STRING, ship_zip_postal_code STRING,
ship_country_region STRING, shipping_fee DECIMAL(19, 4), taxes DECIMAL(19, 4), payment_type STRING,
paid_date TIMESTAMP, notes STRING) ;

create temporary table temp_products(products_key STRING, load_dt TIMESTAMP, mod_dt TIMESTAMP,
mod_type STRING, mod_row_id INT, edl_ingest_channel STRING, edl_ingest_time STRING, deleted BOOLEAN
, product_code STRING, description STRING, standard_cost DECIMAL(19, 4), list_price DECIMAL(19, 4),
reorder_level INT, target_level INT, quantity_per_unit STRING, discontinued INT,
minimum_reorder_quantity INT, category STRING, attachments STRING) ;

create temporary table temp_purchase_order_details(purchase_order_details_key STRING, load_dt
TIMESTAMP, mod_dt TIMESTAMP, mod_type STRING, mod_row_id INT, edl_ingest_channel STRING,
edl_ingest_time STRING, deleted BOOLEAN, quantity DECIMAL(18, 4), unit_cost DECIMAL(19, 4),
date_received TIMESTAMP, posted_to_inventory INT) ;

create temporary table temp_purchase_orders(purchase_orders_key STRING, load_dt TIMESTAMP, mod_dt
TIMESTAMP, mod_type STRING, mod_row_id INT, edl_ingest_channel STRING, edl_ingest_time STRING,
deleted BOOLEAN, submitted_date TIMESTAMP, creation_date TIMESTAMP, status_id INT, expected_date
TIMESTAMP, shipping_fee DECIMAL(19, 4), taxes DECIMAL(19, 4), payment_date TIMESTAMP,
payment_amount DECIMAL(19, 4), payment_method STRING, notes STRING, approved_date TIMESTAMP) ;

create temporary table temp_shippers(shippers_key STRING, load_dt TIMESTAMP, mod_dt TIMESTAMP,
mod_type STRING, mod_row_id INT, edl_ingest_channel STRING, edl_ingest_time STRING, deleted BOOLEAN
, last_name STRING, first_name STRING, email_address STRING, job_title STRING, business_phone
STRING, home_phone STRING, mobile_phone STRING, fax_number STRING, address STRING, city STRING,
state_province STRING, zip_postal_code STRING, country_region STRING, web_page STRING, notes STRING
, attachments STRING) ;

create temporary table temp_suppliers(suppliers_key STRING, load_dt TIMESTAMP, mod_dt TIMESTAMP,
mod_type STRING, mod_row_id INT, edl_ingest_channel STRING, edl_ingest_time STRING, deleted BOOLEAN
, last_name STRING, first_name STRING, email_address STRING, job_title STRING, business_phone
STRING, home_phone STRING, mobile_phone STRING, fax_number STRING, address STRING, city STRING,
state_province STRING, zip_postal_code STRING, country_region STRING, web_page STRING, notes STRING
, attachments STRING) ;

create temporary table temp_l_invoices(link_invoices_key STRING, invoices_key STRING, orders_key
STRING, load_dt TIMESTAMP) ;

create temporary table temp_l_order_details(link_order_details_key STRING, order_details_key STRING
, orders_key STRING, products_key STRING, purchase_orders_key STRING, inventory_transactions_key
STRING, load_dt TIMESTAMP) ;

create temporary table temp_l_orders(link_orders_key STRING, orders_key STRING, employees_key
STRING, customers_key STRING, shippers_key STRING, load_dt TIMESTAMP) ;

create temporary table temp_l_purchase_order_details(link_purchase_order_details_key STRING,
purchase_order_details_key STRING, purchase_orders_key STRING, inventory_transactions_key STRING,
products_key STRING, load_dt TIMESTAMP) ;

create temporary table temp_l_purchase_orders(link_purchase_orders_key STRING, purchase_orders_key
STRING, suppliers_key STRING, created_by_employees_key STRING, approved_by_employees_key STRING,
submitted_by_employees_key STRING, load_dt TIMESTAMP) ;

create temporary table temp_lsat_employee_privileges(link_employee_privileges_key STRING,
employees_key STRING, privileges_key STRING, load_dt TIMESTAMP, mod_dt TIMESTAMP, mod_type STRING,
mod_row_id INT, edl_ingest_channel STRING, edl_ingest_time STRING, deleted BOOLEAN) ;


insert overwrite table temp_customers
select 
    raw.customers_key,
    raw.load_dt,
    raw.mod_dt,
    raw.mod_type,
    raw.mod_row_id,
    raw.edl_ingest_channel,
    raw.edl_ingest_time,
    raw.deleted,
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
      from( select rank() over(partition by customers_key order by mod_dt desc) as rnk, *
            from justin_northwind_raw.s_customers) x
      where rnk = 1) raw
left join justin_northwind_hub.s_customers hub on raw.customers_key = hub.customers_key
where raw.load_dt > hub.load_dt or hub.load_dt is null;

insert overwrite table temp_employees
select 
    raw.employees_key,
    raw.load_dt,
    raw.mod_dt,
    raw.mod_type,
    raw.mod_row_id,
    raw.edl_ingest_channel,
    raw.edl_ingest_time,
    raw.deleted,
    raw.company,
    raw.last_name,
    raw.first_name,
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
      from( select rank() over(partition by employees_key order by mod_dt desc) as rnk, *
            from justin_northwind_raw.s_employees) x
      where rnk = 1) raw
left join justin_northwind_hub.s_employees hub on raw.employees_key = hub.employees_key
where raw.load_dt > hub.load_dt or hub.load_dt is null;

insert overwrite table temp_inventory_transactions
select 
    raw.inventory_transactions_key,
    raw.load_dt,
    raw.mod_dt,
    raw.mod_type,
    raw.mod_row_id,
    raw.edl_ingest_channel,
    raw.edl_ingest_time,
    raw.deleted,
    raw.transaction_type,
    raw.transaction_created_date,
    raw.transaction_modified_date,
    raw.quantity,
    raw.comments
from( select *
      from( select rank() over(partition by inventory_transactions_key order by mod_dt desc) as rnk, *
            from justin_northwind_raw.s_inventory_transactions) x
      where rnk = 1) raw
left join justin_northwind_hub.s_inventory_transactions hub on raw.inventory_transactions_key = hub.inventory_transactions_key
where raw.load_dt > hub.load_dt or hub.load_dt is null;

insert overwrite table temp_invoices
select 
    raw.invoices_key,
    raw.load_dt,
    raw.mod_dt,
    raw.mod_type,
    raw.mod_row_id,
    raw.edl_ingest_channel,
    raw.edl_ingest_time,
    raw.deleted,
    raw.invoice_date,
    raw.due_date,
    raw.tax,
    raw.shipping,
    raw.amount_due
from( select *
      from( select rank() over(partition by invoices_key order by mod_dt desc) as rnk, *
            from justin_northwind_raw.s_invoices) x
      where rnk = 1) raw
left join justin_northwind_hub.s_invoices hub on raw.invoices_key = hub.invoices_key
where raw.load_dt > hub.load_dt or hub.load_dt is null;

insert overwrite table temp_order_details
select 
    raw.order_details_key,
    raw.load_dt,
    raw.mod_dt,
    raw.mod_type,
    raw.mod_row_id,
    raw.edl_ingest_channel,
    raw.edl_ingest_time,
    raw.deleted,
    raw.quantity,
    raw.unit_price,
    raw.discount,
    raw.status_id,
    raw.date_allocated
from( select *
      from( select rank() over(partition by order_details_key order by mod_dt desc) as rnk, *
            from justin_northwind_raw.s_order_details) x
      where rnk = 1) raw
left join justin_northwind_hub.s_order_details hub on raw.order_details_key = hub.order_details_key
where raw.load_dt > hub.load_dt or hub.load_dt is null;

insert overwrite table temp_orders
select 
    raw.orders_key,
    raw.load_dt,
    raw.mod_dt,
    raw.mod_type,
    raw.mod_row_id,
    raw.edl_ingest_channel,
    raw.edl_ingest_time,
    raw.deleted,
    raw.tax_status_id,
    raw.status_id,
    raw.order_date,
    raw.shipped_date,
    raw.ship_name,
    raw.ship_address,
    raw.ship_city,
    raw.ship_state_province,
    raw.ship_zip_postal_code,
    raw.ship_country_region,
    raw.shipping_fee,
    raw.taxes,
    raw.payment_type,
    raw.paid_date,
    raw.notes
from( select *
      from( select rank() over(partition by orders_key order by mod_dt desc) as rnk, *
            from justin_northwind_raw.s_orders) x
      where rnk = 1) raw
left join justin_northwind_hub.s_orders hub on raw.orders_key = hub.orders_key
where raw.load_dt > hub.load_dt or hub.load_dt is null;

insert overwrite table temp_products
select 
    raw.products_key,
    raw.load_dt,
    raw.mod_dt,
    raw.mod_type,
    raw.mod_row_id,
    raw.edl_ingest_channel,
    raw.edl_ingest_time,
    raw.deleted,
    raw.product_code,
    raw.description,
    raw.standard_cost,
    raw.list_price,
    raw.reorder_level,
    raw.target_level,
    raw.quantity_per_unit,
    raw.discontinued,
    raw.minimum_reorder_quantity,
    raw.category,
    raw.attachments
from( select *
      from( select rank() over(partition by products_key order by mod_dt desc) as rnk, *
            from justin_northwind_raw.s_products) x
      where rnk = 1) raw
left join justin_northwind_hub.s_products hub on raw.products_key = hub.products_key
where raw.load_dt > hub.load_dt or hub.load_dt is null;

insert overwrite table temp_purchase_order_details
select 
    raw.purchase_order_details_key,
    raw.load_dt,
    raw.mod_dt,
    raw.mod_type,
    raw.mod_row_id,
    raw.edl_ingest_channel,
    raw.edl_ingest_time,
    raw.deleted,
    raw.quantity,
    raw.unit_cost,
    raw.date_received,
    raw.posted_to_inventory
from( select *
      from( select rank() over(partition by purchase_order_details_key order by load_dt desc) as rnk, *
            from justin_northwind_raw.s_purchase_order_details) x
      where rnk = 1) raw
left join justin_northwind_hub.s_purchase_order_details hub on raw.purchase_order_details_key = hub.purchase_order_details_key
where raw.load_dt > hub.load_dt or hub.load_dt is null;

insert overwrite table temp_purchase_orders
select 
    raw.purchase_orders_key,
    raw.load_dt,
    raw.mod_dt,
    raw.mod_type,
    raw.mod_row_id,
    raw.edl_ingest_channel,
    raw.edl_ingest_time,
    raw.deleted,
    raw.submitted_date,
    raw.creation_date,
    raw.status_id,
    raw.expected_date,
    raw.shipping_fee,
    raw.taxes,
    raw.payment_date,
    raw.payment_amount,
    raw.payment_method,
    raw.notes,
    raw.approved_date
from( select *
      from( select rank() over(partition by purchase_orders_key order by load_dt desc) as rnk, *
            from justin_northwind_raw.s_purchase_orders) x
      where rnk = 1) raw
left join justin_northwind_hub.s_purchase_orders hub on raw.purchase_orders_key = hub.purchase_orders_key
where raw.load_dt > hub.load_dt or hub.load_dt is null;

insert overwrite table temp_shippers
select 
    raw.shippers_key,
    raw.load_dt,
    raw.mod_dt,
    raw.mod_type,
    raw.mod_row_id,
    raw.edl_ingest_channel,
    raw.edl_ingest_time,
    raw.deleted,
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
            from justin_northwind_raw.s_shippers) x
      where rnk = 1) raw
left join justin_northwind_hub.s_shippers hub on raw.shippers_key = hub.shippers_key
where raw.load_dt > hub.load_dt or hub.load_dt is null;

insert overwrite table temp_suppliers
select raw.suppliers_key,
    raw.load_dt,
    raw.mod_dt,
    raw.mod_type,
    raw.mod_row_id,
    raw.edl_ingest_channel,
    raw.edl_ingest_time,
    raw.deleted,
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
      from( select rank() over(partition by suppliers_key order by load_dt desc) as rnk, *
            from justin_northwind_raw.s_suppliers) x
      where rnk = 1) raw
left join justin_northwind_hub.s_suppliers hub on raw.suppliers_key = hub.suppliers_key
where raw.load_dt > hub.load_dt or hub.load_dt is null;

-- ------------------------------------- load links -------------------------------------

insert overwrite table temp_l_invoices
SELECT distinct 
    raw.link_invoices_key,
    raw.invoices_key,
    raw.orders_key,
    raw.load_dt
FROM justin_northwind_raw.l_invoices raw
join ( select distinct invoices_key
       from( select rank() over(partition by invoices_key order by mod_dt desc) as rnk, *
             from justin_northwind_raw.s_invoices) x
       where rnk = 1) s on raw.invoices_key = s.invoices_key
left join justin_northwind_hub.l_invoices hub on raw.link_invoices_key = hub.link_invoices_key
where raw.load_dt > hub.load_dt or hub.load_dt is null;

delete from justin_northwind_hub.l_invoices where exists ( select 1 from justin_northwind_hub.temp_l_invoices temp where l_invoices.invoices_key = temp.invoices_key);

insert into table justin_northwind_hub.l_invoices
SELECT
    link_invoices_key,
    invoices_key,
    orders_key,
    load_dt
FROM justin_northwind_raw.l_invoices
where exists ( select 1 from temp_l_invoices temp
               where l_invoices.invoices_key = temp.invoices_key) ;
               
               
               
               
               
               
               
               
               
               
               
               
-- ------------------------------------- load hubs -------------------------------------

delete from justin_northwind_hub.h_customers where exists ( select 1 from justin_northwind_hub.temp_customers temp where h_customers.customers_key = temp.customers_key and temp.deleted = 1);
delete from justin_northwind_hub.h_employees where exists ( select 1 from justin_northwind_hub.temp_employees temp where h_employees.employees_key = temp.employees_key and temp.deleted = 1);
delete from justin_northwind_hub.h_inventory_transactions where exists ( select 1 from justin_northwind_hub.temp_inventory_transactions temp where h_inventory_transactions.inventory_transactions_key = temp.inventory_transactions_key and temp.deleted = 1);
delete from justin_northwind_hub.h_invoices where exists ( select 1 from justin_northwind_hub.temp_invoices temp where h_invoices.invoices_key = temp.invoices_key and temp.deleted = 1);
delete from justin_northwind_hub.h_order_details where exists ( select 1 from justin_northwind_hub.temp_order_details temp where h_order_details.order_details_key = temp.order_details_key and temp.deleted = 1);
delete from justin_northwind_hub.h_orders where exists ( select 1 from justin_northwind_hub.temp_orders temp where h_orders.orders_key = temp.orders_key and temp.deleted = 1);
delete from justin_northwind_hub.h_products where exists ( select 1 from justin_northwind_hub.temp_products temp where h_products.products_key = temp.products_key and temp.deleted = 1);
delete from justin_northwind_hub.h_purchase_order_details where exists ( select 1 from justin_northwind_hub.temp_purchase_order_details temp where h_purchase_order_details.purchase_order_details_key = temp.purchase_order_details_key and temp.deleted = 1);
delete from justin_northwind_hub.h_purchase_orders where exists ( select 1 from justin_northwind_hub.temp_purchase_orders temp where h_purchase_orders.purchase_orders_key = temp.purchase_orders_key and temp.deleted = 1);
delete from justin_northwind_hub.h_shippers where exists ( select 1 from justin_northwind_hub.temp_shippers temp where h_shippers.shippers_key = temp.shippers_key and temp.deleted = 1);
delete from justin_northwind_hub.h_suppliers where exists ( select 1 from justin_northwind_hub.temp_suppliers temp where h_suppliers.suppliers_key = temp.suppliers_key and temp.deleted = 1);



insert into table justin_northwind_hub.h_customers
SELECT
    customers_key,
    customer_id,
    company,
    load_dt
FROM justin_northwind_raw.h_customers
where exists ( select 1 from temp_customers temp
               where h_customers.customers_key = temp.customers_key) ;

insert into table justin_northwind_hub.h_employees
SELECT
    employees_key,
    employee_id,
    email_address,
    load_dt
FROM justin_northwind_raw.h_employees
where exists ( select 1 from  temp_employees temp 
               where h_employees.employees_key = temp.employees_key) ;
            
insert into table justin_northwind_hub.h_inventory_transactions
SELECT
    inventory_transactions_key,
    inventory_transaction_id,
    load_dt
FROM
    justin_northwind_raw.h_inventory_transactions
where exists ( select 1 from temp_inventory_transactions temp
               where h_inventory_transactions.inventory_transactions_key = temp.inventory_transactions_key);

insert into table justin_northwind_hub.h_invoices
SELECT
    invoices_key,
    invoice_id,
    load_dt
FROM justin_northwind_raw.h_invoices
where exists ( select 1 from temp_invoices temp
               where h_invoices.invoices_key = temp.invoices_key) ;
            
insert into table justin_northwind_hub.h_order_details
SELECT
    order_details_key,
    order_detail_id,
    load_dt
FROM justin_northwind_raw.h_order_details
where exists ( select 1 from temp_order_details temp
               where h_order_details.order_details_key = temp.order_details_key) ;

insert into table justin_northwind_hub.h_orders
SELECT
    orders_key,
    order_id,
    load_dt
FROM justin_northwind_raw.h_orders
where exists ( select 1 from temp_orders temp
      where h_orders.orders_key = temp.orders_key) ;

insert into table justin_northwind_hub.h_products
SELECT
    products_key,
    product_id,
    product_name,
    load_dt
FROM justin_northwind_raw.h_products
where exists ( select 1 from temp_products temp
               where h_products.products_key = temp.products_key) ;

insert into table justin_northwind_hub.h_purchase_order_details
SELECT
    purchase_order_details_key,
    purchase_order_detail_id,
    load_dt
FROM justin_northwind_raw.h_purchase_order_details
where exists ( select 1 from temp_purchase_order_details temp
               where h_purchase_order_details.purchase_order_details_key = temp.purchase_order_details_key);

insert into table justin_northwind_hub.h_purchase_orders
SELECT
    purchase_orders_key,
    purchase_order_id,
    load_dt
FROM justin_northwind_raw.h_purchase_orders
where exists ( select 1 from temp_purchase_orders temp
               where h_purchase_orders.purchase_orders_key = temp.purchase_orders_key) ;

insert into table justin_northwind_hub.h_shippers
SELECT
    shippers_key,
    shipper_id,
    company,
    load_dt
FROM justin_northwind_raw.h_shippers
where exists ( select 1 from temp_shippers temp
               where h_shippers.shippers_key = temp.shippers_key) ;

insert into table justin_northwind_hub.h_suppliers
SELECT
    suppliers_key,
    supplier_id,
    company,
    load_dt
FROM justin_northwind_raw.h_suppliers
where exists ( select 1 from temp_suppliers temp
               where h_suppliers.suppliers_key = temp.suppliers_key) ;

insert into table justin_northwind_hub.h_privileges
SELECT
    privileges_key,
    privilege_id,
    privilege_name,
    load_dt
FROM justin_northwind_raw.h_privileges
where exists ( select 1 from temp_privileges temp
               where h_privileges.privileges_key = temp.privileges_key) ;
               
-- ------------------------------------- load satellites -------------------------------------

delete from justin_northwind_hub.s_customers where exists ( select 1 from justin_northwind_hub.temp_customers temp where s_customers.customers_key = temp.customers_key);
delete from justin_northwind_hub.s_employees where exists ( select 1 from justin_northwind_hub.temp_employees temp where s_employees.employees_key = temp.employees_key);
delete from justin_northwind_hub.s_inventory_transactions where exists ( select 1 from justin_northwind_hub.temp_inventory_transactions temp where s_inventory_transactions.inventory_transactions_key = temp.inventory_transactions_key);
delete from justin_northwind_hub.s_invoices where exists ( select 1 from justin_northwind_hub.temp_invoices temp where s_invoices.invoices_key = temp.invoices_key);
delete from justin_northwind_hub.s_order_details where exists ( select 1 from justin_northwind_hub.temp_order_details temp where s_order_details.order_details_key = temp.order_details_key);
delete from justin_northwind_hub.s_orders where exists ( select 1 from justin_northwind_hub.temp_orders temp where s_orders.orders_key = temp.orders_key);
delete from justin_northwind_hub.s_products where exists ( select 1 from justin_northwind_hub.temp_products temp where s_products.products_key = temp.products_key);
delete from justin_northwind_hub.s_purchase_order_details where exists ( select 1 from justin_northwind_hub.temp_purchase_order_details temp where s_purchase_order_details.purchase_order_details_key = temp.purchase_order_details_key);
delete from justin_northwind_hub.s_purchase_orders where exists ( select 1 from justin_northwind_hub.temp_purchase_orders temp where s_purchase_orders.purchase_orders_key = temp.purchase_orders_key);
delete from justin_northwind_hub.s_shippers where exists ( select 1 from justin_northwind_hub.temp_shippers temp where s_shippers.shippers_key = temp.shippers_key);
delete from justin_northwind_hub.s_suppliers where exists ( select 1 from justin_northwind_hub.temp_suppliers temp where s_suppliers.suppliers_key = temp.suppliers_key);

insert into table justin_northwind_hub.s_customers 
select
    customer_key,
    load_dt,
    mod_dt,
    mod_type,
    mod_row_id,
    edl_ingest_channel,
    edl_ingest_time,
    deleted,
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
from temp_customers 
where deleted = 0;

insert into table justin_northwind_hub.s_employees 
select
    employees_key,
    load_dt,
    mod_dt,
    mod_type,
    mod_row_id,
    edl_ingest_channel,
    edl_ingest_time,
    deleted,
    company,
    last_name,
    first_name,
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
from temp_employees 
where deleted = 0;

insert into table justin_northwind_hub.s_inventory_transactions 
select
    inventory_transactions_key,
    load_dt,
    mod_dt,
    mod_type,
    mod_row_id,
    edl_ingest_channel,
    edl_ingest_time,
    deleted,
    transaction_type,
    transaction_created_date,
    transaction_modified_date,
    quantity,
    comments
from temp_inventory_transactions 
where deleted = 0;

insert into table justin_northwind_hub.s_invoices 
select
    invoices_key,
    load_dt,
    mod_dt,
    mod_type,
    mod_row_id,
    edl_ingest_channel,
    edl_ingest_time,
    deleted,
    invoice_date,
    due_date,
    tax,
    shipping,
    amount_due
from temp_invoices 
where deleted = 0;

insert into table justin_northwind_hub.s_order_details 
select
    order_details_key,
    load_dt,
    mod_dt,
    mod_type,
    mod_row_id,
    edl_ingest_channel,
    edl_ingest_time,
    deleted,
    quantity,
    unit_price,
    discount,
    status_id,
    date_allocated
from temp_order_details 
where deleted = 0;

insert into table justin_northwind_hub.s_orders 
select
    orders_key,
    load_dt,
    mod_dt,
    mod_type,
    mod_row_id,
    edl_ingest_channel,
    edl_ingest_time,
    deleted,
    tax_status_id,
    status_id,
    order_date,
    shipped_date,
    ship_name,
    ship_address,
    ship_city,
    ship_state_province,
    ship_zip_postal_code,
    ship_country_region,
    shipping_fee,
    taxes,
    payment_type,
    paid_date,
    notes
from temp_orders 
where deleted = 0;

insert into table justin_northwind_hub.s_products 
select
    products_key,
    load_dt,
    mod_dt,
    mod_type,
    mod_row_id,
    edl_ingest_channel,
    edl_ingest_time,
    deleted,
    product_code,
    description,
    standard_cost,
    list_price,
    reorder_level,
    target_level,
    quantity_per_unit,
    discontinued,
    minimum_reorder_quantity,
    category,
    attachments
from temp_products where deleted = 0;

insert into table justin_northwind_hub.s_purchase_order_details 
select
    purchase_order_details_key,
    load_dt,
    mod_dt,
    mod_type,
    mod_row_id,
    edl_ingest_channel,
    edl_ingest_time,
    deleted,
    quantity,
    unit_cost,
    date_received,
    posted_to_inventory
from temp_purchase_order_details 
where deleted = 0;

insert into table justin_northwind_hub.s_purchase_orders 
select
    purchase_orders_key,
    load_dt,
    mod_dt,
    mod_type,
    mod_row_id,
    edl_ingest_channel,
    edl_ingest_time,
    deleted,
    submitted_date,
    creation_date,
    status_id,
    expected_date,
    shipping_fee,
    taxes,
    payment_date,
    payment_amount,
    payment_method,
    notes,
    approved_date
from temp_purchase_orders 
where deleted = 0;

insert into table justin_northwind_hub.s_shippers 
select
    shippers_key,
    load_dt,
    mod_dt,
    mod_type,
    mod_row_id,
    edl_ingest_channel,
    edl_ingest_time,
    deleted,
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
from temp_shippers 
where deleted = 0;

insert into table justin_northwind_hub.s_suppliers 
select
    suppliers_key,
    load_dt,
    mod_dt,
    mod_type,
    mod_row_id,
    edl_ingest_channel,
    edl_ingest_time,
    deleted,
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
from temp_suppliers 
where deleted = 0;

-- ------------------------------------- load links -------------------------------------




delete
from justin_northwind_hub.l_invoices raw 
where exists ( select 1 
               from justin_northwind_hub.l_invoices hub
               where raw.link_invoices_key = hub.link_invoices_key
               and raw.load_dt > hub.load_dt);

delete
from justin_northwind_hub.l_order_details raw 
where exists ( select 1 
               from justin_northwind_hub.l_order_details hub
               where raw.link_order_details_key = hub.link_order_details_key
               and raw.load_dt > hub.load_dt);

delete
from justin_northwind_hub.l_orders raw 
where exists ( select 1 
               from justin_northwind_hub.l_orders hub
               where raw.link_orders_key = hub.link_orders_key
               and raw.load_dt > hub.load_dt);

delete
from justin_northwind_hub.l_purchase_order_details raw 
where exists ( select 1 
               from justin_northwind_hub.l_purchase_order_details hub
               where raw.link_order_purchase_details_key = hub.link_purchase_order_details_key
               and raw.load_dt > hub.load_dt);
               
delete
from justin_northwind_hub.l_purchase_orders raw 
where exists ( select 1 
               from justin_northwind_hub.l_purchase_orders hub
               where raw.link_puchase_orders_key = hub.link_puchase_orders_key
               and raw.load_dt > hub.load_dt);    
               
insert into table justin_northwind_hub.l_employee_privileges
select 
    link_employee_privileges_key, 
    employees_key,
    privileges_key,
    load_dt
from justin_northwind_raw.l_employee_privileges raw
where not exists ( select 1 
                   from justin_northwind_hub.l_employee_privileges hub
                   where raw.link_employee_privileges_key = hub.link_employee_privileges_key)





-- ------------------------------------- load reference tables -------------------------------------

insert overwrite table justin_northwind_hub.r_inventory_transaction_types
select load_dt, mod_dt, mod_type, mod_row_id, edl_ingest_channel, edl_ingest_time, deleted, inventory_transaction_type_id, type_name 
from ( select rank() over(partition by inventory_transaction_type_id order by mod_dt desc) as rnk, *
       from justin_northwind_raw.r_inventory_transaction_types) x
where rnk = 1 and deleted = 0;

insert overwrite table justin_northwind_hub.r_order_details_status 
select load_dt, mod_dt, mod_type, mod_row_id, edl_ingest_channel, edl_ingest_time, deleted, order_details_status_id, status_name 
from ( select rank() over(partition by order_details_status_id order by mod_dt desc) as rnk, *
       from justin_northwind_raw.r_order_details_status) x
where rnk = 1 and deleted = 0;

insert overwrite table justin_northwind_hub.r_orders_status 
select load_dt, mod_dt, mod_type, mod_row_id, edl_ingest_channel, edl_ingest_time, deleted, order_status_id, status_name 
from ( select rank() over(partition by order_status_id order by mod_dt desc) as rnk, *
       from justin_northwind_raw.r_orders_status) x
where rnk = 1 and deleted = 0;

insert overwrite table justin_northwind_hub.r_orders_tax_status 
select load_dt, mod_dt, mod_type, mod_row_id, edl_ingest_channel, edl_ingest_time, deleted, order_tax_status_id, tax_status_name 
from ( select rank() over(partition by order_tax_status_id order by mod_dt desc) as rnk, *
       from justin_northwind_raw.r_orders_tax_status) x
where rnk = 1 and deleted = 0;

insert overwrite table justin_northwind_hub.r_purchase_order_status 
select load_dt, mod_dt, mod_type, mod_row_id, edl_ingest_channel, edl_ingest_time, deleted, purchase_order_status_id, status 
from ( select rank() over(partition by purchase_order_status_id order by mod_dt desc) as rnk, *
       from justin_northwind_raw.r_purchase_order_status) x
where rnk = 1 and deleted = 0;

insert overwrite table justin_northwind_hub.r_sales_reports 
select load_dt, mod_dt, mod_type, mod_row_id, edl_ingest_channel, edl_ingest_time, deleted, group_by, display, title, filter_row_source, default 
from ( select rank() over(partition by group_by, display, title, filter_row_source, default order by mod_dt desc) as rnk, *
       from justin_northwind_raw.r_sales_reports) x
where rnk = 1;

insert overwrite table justin_northwind_hub.r_strings 
select load_dt, mod_dt, mod_type, mod_row_id, edl_ingest_channel, edl_ingest_time, deleted, string_id, string_data 
from ( select rank() over(partition by string_id order by mod_dt desc) as rnk, *
       from justin_northwind_raw.r_strings) x
where rnk = 1 and deleted = 0;


















