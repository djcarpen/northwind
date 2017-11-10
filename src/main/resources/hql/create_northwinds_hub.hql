use justin_northwind_hub;

drop table if exists h_customers;
drop table if exists h_employees;
drop table if exists h_inventory_transactions;
drop table if exists h_invoices;
drop table if exists h_order_details;
drop table if exists h_orders;
drop table if exists h_privileges;
drop table if exists h_products;
drop table if exists h_purchase_order_details;
drop table if exists h_purchase_orders;
drop table if exists h_shippers;
drop table if exists h_suppliers;
drop table if exists l_invoices;
drop table if exists l_orders;
drop table if exists l_order_details;
drop table if exists l_purchase_orders;
drop table if exists l_purchase_order_details;
drop table if exists r_inventory_transaction_types;
drop table if exists r_order_details_status;
drop table if exists r_orders_status;
drop table if exists r_orders_tax_status;
drop table if exists r_purchase_order_status;
drop table if exists r_sales_reports;
drop table if exists r_strings;
drop table if exists s_customers;
drop table if exists s_employees;
drop table if exists s_inventory_transactions;
drop table if exists s_invoices;
drop table if exists s_orders;
drop table if exists s_order_details;
drop table if exists s_products;
drop table if exists s_purchase_orders;
drop table if exists s_purchase_order_details;
drop table if exists s_shippers;
drop table if exists s_suppliers;
drop table if exists h_employee_privileges;
drop table if exists s_employee_privileges;
drop table if exists l_employee_privileges;

CREATE TABLE H_customers (
  customers_key STRING,
  customer_id INT ,
  company STRING,
  load_dt TIMESTAMP) 
clustered by (customers_key) into 1 buckets
stored as ORC
tblproperties ("transactional"="true");
  
CREATE TABLE H_privileges (
  privileges_key STRING,
  privilege_id INT ,
  privilege_name STRING,
  load_dt TIMESTAMP)
clustered by (privileges_key) into 1 buckets
stored as ORC
tblproperties ("transactional"="true");

CREATE TABLE H_employees (
  employees_key STRING,
  employee_id INT ,
  email_address STRING,
  load_dt TIMESTAMP) 
clustered by (employees_key) into 1 buckets
stored as ORC
tblproperties ("transactional"="true");

CREATE TABLE H_suppliers (
  suppliers_key STRING,
  supplier_id INT ,
  company STRING,
  load_dt TIMESTAMP)
clustered by (suppliers_key) into 1 buckets
stored as ORC
tblproperties ("transactional"="true");

CREATE TABLE H_shippers (
  shippers_key STRING,
  shipper_id INT ,
  company STRING,
  load_dt TIMESTAMP) 
clustered by (shippers_key) into 1 buckets
stored as ORC
tblproperties ("transactional"="true");

CREATE TABLE H_products (
  products_key STRING,
  product_id int,
  product_name STRING,
  load_dt TIMESTAMP) 
clustered by (products_key) into 1 buckets
stored as ORC
tblproperties ("transactional"="true");

CREATE TABLE H_invoices (
  invoices_key STRING,
  invoice_id INT ,
  load_dt TIMESTAMP) 
clustered by (invoices_key) into 1 buckets
stored as ORC
tblproperties ("transactional"="true");

CREATE TABLE H_orders (
  orders_key STRING,
  order_id INT ,
  load_dt TIMESTAMP) 
clustered by (orders_key) into 1 buckets
stored as ORC
tblproperties ("transactional"="true");

CREATE TABLE H_order_details (
  order_details_key STRING, 
  order_detail_id INT,
  load_dt TIMESTAMP) 
clustered by (order_details_key) into 1 buckets
stored as ORC
tblproperties ("transactional"="true");

CREATE TABLE H_purchase_orders (
  purchase_orders_key STRING,
  purchase_order_id INT ,
  load_dt TIMESTAMP) 
clustered by (purchase_orders_key) into 1 buckets
stored as ORC
tblproperties ("transactional"="true");

CREATE TABLE H_purchase_order_details (
  purchase_order_details_key STRING,
  purchase_order_detail_id INT ,
  load_dt TIMESTAMP) 
clustered by (purchase_order_details_key) into 1 buckets
stored as ORC
tblproperties ("transactional"="true");

CREATE TABLE H_inventory_transactions (
  inventory_transactions_key STRING,
  inventory_transaction_id INT ,
  load_dt TIMESTAMP) 
clustered by (inventory_transactions_key) into 1 buckets
stored as ORC
tblproperties ("transactional"="true");

CREATE TABLE S_customers (
  customers_key STRING,
  load_dt TIMESTAMP,
  dtl__capxtimestamp TIMESTAMP,
  dtl__capxaction STRING,
  dtl__capxrowid INT,
  edl_ingest_channel STRING,
  edl_ingest_time STRING,
  edl_soft_delete BOOLEAN,
  edl_source_file STRING,
  last_name STRING,
  first_name STRING,
  email_address STRING,
  job_title STRING,
  business_phone STRING,
  home_phone STRING,
  mobile_phone STRING,
  fax_number STRING,
  address STRING,
  city STRING,
  state_province STRING,
  zip_postal_code STRING,
  country_region STRING,
  web_page STRING,
  notes STRING,
  attachments STRING) 
clustered by (customers_key) into 1 buckets
stored as ORC
tblproperties ("transactional"="true");

CREATE TABLE S_employees (
  employees_key STRING,
  load_dt TIMESTAMP,
  dtl__capxtimestamp TIMESTAMP,
  dtl__capxaction STRING,
  dtl__capxrowid INT,
  edl_ingest_channel STRING,
  edl_ingest_time STRING,
  edl_soft_delete BOOLEAN,
  edl_source_file STRING,
  company STRING,
  last_name STRING,
  first_name STRING,
  job_title STRING,
  business_phone STRING,
  home_phone STRING,
  mobile_phone STRING,
  fax_number STRING,
  address STRING,
  city STRING,
  state_province STRING,
  zip_postal_code STRING,
  country_region STRING,
  web_page STRING,
  notes STRING,
  attachments STRING) 
clustered by (employees_key) into 1 buckets
stored as ORC
tblproperties ("transactional"="true");

CREATE TABLE S_shippers (
  shippers_key STRING,
  load_dt TIMESTAMP,
  dtl__capxtimestamp TIMESTAMP,
  dtl__capxaction STRING,
  dtl__capxrowid INT,
  edl_ingest_channel STRING,
  edl_ingest_time STRING,
  edl_soft_delete BOOLEAN,
  edl_source_file STRING,
  last_name STRING,
  first_name STRING,
  email_address STRING,
  job_title STRING,
  business_phone STRING,
  home_phone STRING,
  mobile_phone STRING,
  fax_number STRING,
  address STRING,
  city STRING,
  state_province STRING,
  zip_postal_code STRING,
  country_region STRING,
  web_page STRING,
  notes STRING,
  attachments STRING) 
clustered by (shippers_key) into 1 buckets
stored as ORC
tblproperties ("transactional"="true");

CREATE TABLE S_orders (
  orders_key STRING,
  load_dt TIMESTAMP,
  dtl__capxtimestamp TIMESTAMP,
  dtl__capxaction STRING,
  dtl__capxrowid INT,
  edl_ingest_channel STRING,
  edl_ingest_time STRING,
  edl_soft_delete BOOLEAN,
  edl_source_file STRING,
  tax_status_id INT,
  status_id INT,
  order_date TIMESTAMP,
  shipped_date TIMESTAMP,
  ship_name STRING,
  ship_address STRING,
  ship_city STRING,
  ship_state_province STRING,
  ship_zip_postal_code STRING,
  ship_country_region STRING,
  shipping_fee DECIMAL(19,4),
  taxes DECIMAL(19,4),
  payment_type STRING,
  paid_date TIMESTAMP,
  notes STRING) 
clustered by (orders_key) into 1 buckets
stored as ORC
tblproperties ("transactional"="true");

CREATE TABLE S_order_details (
  order_details_key STRING,
  load_dt TIMESTAMP,
  dtl__capxtimestamp TIMESTAMP,
  dtl__capxaction STRING,
  dtl__capxrowid INT,
  edl_ingest_channel STRING,
  edl_ingest_time STRING,
  edl_soft_delete BOOLEAN,
  edl_source_file STRING,
  quantity DECIMAL(18,4),
  unit_price DECIMAL(19,4),
  discount DOUBLE,
  status_id INT,
  date_allocated TIMESTAMP)
clustered by (order_details_key) into 1 buckets
stored as ORC
tblproperties ("transactional"="true");

CREATE TABLE S_products (
  products_key STRING,
  load_dt TIMESTAMP,
  dtl__capxtimestamp TIMESTAMP,
  dtl__capxaction STRING,
  dtl__capxrowid INT,
  edl_ingest_channel STRING,
  edl_ingest_time STRING,
  edl_soft_delete BOOLEAN,
  edl_source_file STRING,
  product_code STRING,
  description STRING,
  standard_cost DECIMAL(19,4),
  list_price DECIMAL(19,4),
  reorder_level INT,
  target_level INT,
  quantity_per_unit STRING,
  discontinued INT,
  minimum_reorder_quantity INT,
  category STRING,
  attachments STRING) 
clustered by (products_key) into 1 buckets
stored as ORC
tblproperties ("transactional"="true");

CREATE TABLE S_suppliers (
  suppliers_key STRING,
  load_dt TIMESTAMP,
  dtl__capxtimestamp TIMESTAMP,
  dtl__capxaction STRING,
  dtl__capxrowid INT,
  edl_ingest_channel STRING,
  edl_ingest_time STRING,
  edl_soft_delete BOOLEAN,
  edl_source_file STRING,
  last_name STRING,
  first_name STRING,
  email_address STRING,
  job_title STRING,
  business_phone STRING,
  home_phone STRING,
  mobile_phone STRING,
  fax_number STRING,
  address STRING,
  city STRING,
  state_province STRING,
  zip_postal_code STRING,
  country_region STRING,
  web_page STRING,
  notes STRING,
  attachments STRING) 
clustered by (suppliers_key) into 1 buckets
stored as ORC
tblproperties ("transactional"="true");

CREATE TABLE S_purchase_orders (
  purchase_orders_key STRING,
  load_dt TIMESTAMP,
  dtl__capxtimestamp TIMESTAMP,
  dtl__capxaction STRING,
  dtl__capxrowid INT,
  edl_ingest_channel STRING,
  edl_ingest_time STRING,
  edl_soft_delete BOOLEAN,
  edl_source_file STRING,
  submitted_date TIMESTAMP,
  creation_date TIMESTAMP,
  status_id INT,
  expected_date TIMESTAMP,
  shipping_fee DECIMAL(19,4),
  taxes DECIMAL(19,4),
  payment_date TIMESTAMP,
  payment_amount DECIMAL(19,4),
  payment_method STRING,
  notes STRING,
  approved_date TIMESTAMP) 
clustered by (purchase_orders_key) into 1 buckets
stored as ORC
tblproperties ("transactional"="true");

CREATE TABLE S_purchase_order_details (
  purchase_order_details_key STRING,
  load_dt TIMESTAMP,
  dtl__capxtimestamp TIMESTAMP,
  dtl__capxaction STRING,
  dtl__capxrowid INT,
  edl_ingest_channel STRING,
  edl_ingest_time STRING,
  edl_soft_delete BOOLEAN,
  edl_source_file STRING,
  quantity DECIMAL(18,4),
  unit_cost DECIMAL(19,4),
  date_received TIMESTAMP,
  posted_to_inventory INT) 
clustered by (purchase_order_details_key) into 1 buckets
stored as ORC
tblproperties ("transactional"="true");

CREATE TABLE S_inventory_transactions (
  inventory_transactions_key STRING,
  load_dt TIMESTAMP,
  dtl__capxtimestamp TIMESTAMP,
  dtl__capxaction STRING,
  dtl__capxrowid INT,
  edl_ingest_channel STRING,
  edl_ingest_time STRING,
  edl_soft_delete BOOLEAN,
  edl_source_file STRING,
  transaction_type INT,
  transaction_created_date TIMESTAMP,
  transaction_modified_date TIMESTAMP,
  quantity INT,
  comments STRING) 
clustered by (inventory_transactions_key) into 1 buckets
stored as ORC
tblproperties ("transactional"="true");

CREATE TABLE S_invoices (
  invoices_key STRING,
  load_dt TIMESTAMP,
  dtl__capxtimestamp TIMESTAMP,
  dtl__capxaction STRING,
  dtl__capxrowid INT,
  edl_ingest_channel STRING,
  edl_ingest_time STRING,
  edl_soft_delete BOOLEAN,
  edl_source_file STRING,
  invoice_date TIMESTAMP,
  due_date TIMESTAMP,
  tax DECIMAL(19,4),
  shipping DECIMAL(19,4),
  amount_due DECIMAL(19,4)) 
clustered by (invoices_key) into 1 buckets
stored as ORC
tblproperties ("transactional"="true");

CREATE TABLE L_purchase_orders (
  link_purchase_orders_key STRING,
  purchase_orders_key STRING,
  suppliers_key STRING,
  created_by_employees_key STRING,
  approved_by_employees_key STRING,
  submitted_by_employees_key STRING,
  load_dt TIMESTAMP) 
clustered by (link_purchase_orders_key) into 1 buckets
stored as ORC
tblproperties ("transactional"="true");

CREATE TABLE L_orders (
  link_orders_key STRING,
  orders_key STRING,
  employees_key STRING,
  customers_key STRING,
  shippers_key STRING,
  load_dt TIMESTAMP)
clustered by (link_orders_key) into 1 buckets
stored as ORC
tblproperties ("transactional"="true");

CREATE TABLE L_order_details (
  link_order_details_key STRING,
  order_details_key STRING,
  orders_key STRING,
  products_key STRING,
  purchase_orders_key STRING,
  inventory_transactions_key STRING,
  load_dt TIMESTAMP) 
clustered by (link_order_details_key) into 1 buckets
stored as ORC
tblproperties ("transactional"="true");

CREATE TABLE L_invoices (
  link_invoices_key STRING,
  invoices_key STRING,
  orders_key STRING,
  load_dt TIMESTAMP)
clustered by (link_invoices_key) into 1 buckets
stored as ORC
tblproperties ("transactional"="true");

CREATE TABLE L_purchase_order_details (
  link_purchase_order_details_key STRING,
  purchase_order_details_key STRING,
  purchase_orders_key STRING,
  inventory_transactions_key STRING,
  products_key STRING,
  load_dt TIMESTAMP) 
clustered by (link_purchase_order_details_key) into 1 buckets
stored as ORC
tblproperties ("transactional"="true");
  
CREATE TABLE R_purchase_order_status (
  load_dt TIMESTAMP,
  dtl__capxtimestamp TIMESTAMP,
  dtl__capxaction STRING,
  dtl__capxrowid INT,
  edl_ingest_channel STRING,
  edl_ingest_time STRING,
  edl_soft_delete BOOLEAN,
  edl_source_file STRING,
  purchase_order_status_id INT,
  status STRING) 
stored as ORC;

CREATE TABLE R_inventory_transaction_types (
  load_dt TIMESTAMP,
  dtl__capxtimestamp TIMESTAMP,
  dtl__capxaction STRING,
  dtl__capxrowid INT,
  edl_ingest_channel STRING,
  edl_ingest_time STRING,
  edl_soft_delete BOOLEAN,
  edl_source_file STRING,
  inventory_transaction_type_id INT,
  type_name STRING) 
stored as ORC;

CREATE TABLE R_order_details_status (
  load_dt TIMESTAMP,
  dtl__capxtimestamp TIMESTAMP,
  dtl__capxaction STRING,
  dtl__capxrowid INT,
  edl_ingest_channel STRING,
  edl_ingest_time STRING,
  edl_soft_delete BOOLEAN,
  edl_source_file STRING,
  order_details_status_id INT,
  status_name STRING) 
stored as ORC;

CREATE TABLE R_orders_tax_status (
  load_dt TIMESTAMP,
  dtl__capxtimestamp TIMESTAMP,
  dtl__capxaction STRING,
  dtl__capxrowid INT,
  edl_ingest_channel STRING,
  edl_ingest_time STRING,
  edl_soft_delete BOOLEAN,
  edl_source_file STRING,
  order_tax_status_id INT,
  tax_status_name STRING) 
stored as ORC;

CREATE TABLE R_orders_status (
  load_dt TIMESTAMP,
  dtl__capxtimestamp TIMESTAMP,
  dtl__capxaction STRING,
  dtl__capxrowid INT,
  edl_ingest_channel STRING,
  edl_ingest_time STRING,
  edl_soft_delete BOOLEAN,
  edl_source_file STRING,
  order_status_id INT,
  status_name STRING)
stored as ORC;

CREATE TABLE R_sales_reports (
  load_dt TIMESTAMP,
  dtl__capxtimestamp TIMESTAMP,
  dtl__capxaction STRING,
  dtl__capxrowid INT,
  edl_ingest_channel STRING,
  edl_ingest_time STRING,
  edl_soft_delete BOOLEAN,
  edl_source_file STRING,
  group_by STRING,
  display STRING,
  title STRING,
  filter_row_source STRING,
  `default` INT) 
stored as ORC;

CREATE TABLE R_strings (
  load_dt TIMESTAMP,
  dtl__capxtimestamp TIMESTAMP,
  dtl__capxaction STRING,
  dtl__capxrowid INT,
  edl_ingest_channel STRING,
  edl_ingest_time STRING,
  edl_soft_delete BOOLEAN,
  edl_source_file STRING,
  string_id INT ,
  string_data STRING) 
stored as ORC;

CREATE TABLE H_employee_privileges (
  employee_privileges_key STRING,
  employees_key STRING,
  privileges_key STRING,
  load_dt TIMESTAMP) 
clustered by (employee_privileges_key) into 1 buckets
stored as ORC
tblproperties ("transactional"="true");

  
CREATE TABLE L_employee_privileges (
  link_employee_privileges_key STRING,
  employee_privileges_key STRING,
  employees_key STRING,
  privileges_key STRING,
  load_dt TIMESTAMP) 
clustered by (link_employee_privileges_key) into 1 buckets
stored as ORC
tblproperties ("transactional"="true");

  
CREATE TABLE S_employee_privileges (
  employee_privileges_key STRING,
  load_dt TIMESTAMP,
  dtl__capxtimestamp TIMESTAMP,
  dtl__capxaction STRING,
  dtl__capxrowid INT,
  edl_ingest_channel STRING,
  edl_ingest_time STRING,
  edl_soft_delete BOOLEAN,
  edl_source_file STRING) 
clustered by (employee_privileges_key) into 1 buckets
stored as ORC
tblproperties ("transactional"="true");