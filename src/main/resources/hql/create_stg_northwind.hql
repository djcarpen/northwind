
use ${hivevar:databaseName};

CREATE EXTERNAL TABLE stg_northwind_customers (
  DTL__CAPXTIMESTAMP STRING,
  DTL__CAPXACTION STRING,
  DTL__CAPXROWID INT,
  id INT ,
  company STRING,
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
  attachments STRING  
)
partitioned by (EDL_INGEST_CHANNEL STRING, EDL_INGEST_TIME STRING)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\u001F'
STORED AS TEXTFILE
LOCATION 'hdfs:///user/rn185083/northwind/customers/'
;

CREATE EXTERNAL TABLE stg_northwind_employees (
  DTL__CAPXTIMESTAMP STRING,
  DTL__CAPXACTION STRING,
  DTL__CAPXROWID INT,
  id INT ,
  company STRING,
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
  attachments STRING  
)
partitioned by (EDL_INGEST_CHANNEL STRING, EDL_INGEST_TIME STRING)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\u001F'
STORED AS TEXTFILE
LOCATION 'hdfs:///user/rn185083/northwind/employees'
;

CREATE EXTERNAL TABLE stg_northwind_privileges (
  DTL__CAPXTIMESTAMP STRING,
  DTL__CAPXACTION STRING,
  DTL__CAPXROWID INT,
  id INT ,
  privilege_name STRING  
)
partitioned by (EDL_INGEST_CHANNEL STRING, EDL_INGEST_TIME STRING)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\u001F'
STORED AS TEXTFILE
LOCATION 'hdfs:///user/rn185083/northwind/privileges'
;

CREATE EXTERNAL TABLE stg_northwind_employee_privileges (
  DTL__CAPXTIMESTAMP STRING,
  DTL__CAPXACTION STRING,
  DTL__CAPXROWID INT,
  employee_id INT,
  privilege_id INT  
)
partitioned by (EDL_INGEST_CHANNEL STRING, EDL_INGEST_TIME STRING)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\u001F'
STORED AS TEXTFILE
LOCATION 'hdfs:///user/rn185083/northwind/employee_privileges'
;

CREATE EXTERNAL TABLE stg_northwind_inventory_transaction_types (
  DTL__CAPXTIMESTAMP STRING,
  DTL__CAPXACTION STRING,
  DTL__CAPXROWID INT,
  id INT,
  type_name STRING  
)
partitioned by (EDL_INGEST_CHANNEL STRING, EDL_INGEST_TIME STRING)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\u001F'
STORED AS TEXTFILE
LOCATION 'hdfs:///user/rn185083/northwind/inventory_transaction_types'
;

CREATE EXTERNAL TABLE stg_northwind_shippers (
  DTL__CAPXTIMESTAMP STRING,
  DTL__CAPXACTION STRING,
  DTL__CAPXROWID INT,
  id INT ,
  company STRING,
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
  attachments STRING  
)
partitioned by (EDL_INGEST_CHANNEL STRING, EDL_INGEST_TIME STRING)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\u001F'
STORED AS TEXTFILE
LOCATION 'hdfs:///user/rn185083/northwind/shippers'
;

CREATE EXTERNAL TABLE stg_northwind_orders_tax_status (
  DTL__CAPXTIMESTAMP STRING,
  DTL__CAPXACTION STRING,
  DTL__CAPXROWID INT,
  id INT,
  tax_status_name STRING  
)
partitioned by (EDL_INGEST_CHANNEL STRING, EDL_INGEST_TIME STRING)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\u001F'
STORED AS TEXTFILE
LOCATION 'hdfs:///user/rn185083/northwind/orders_tax_status'
;

CREATE EXTERNAL TABLE stg_northwind_orders_status (
  DTL__CAPXTIMESTAMP STRING,
  DTL__CAPXACTION STRING,
  DTL__CAPXROWID INT,
  id INT,
  status_name STRING  
)
partitioned by (EDL_INGEST_CHANNEL STRING, EDL_INGEST_TIME STRING)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\u001F'
STORED AS TEXTFILE
LOCATION 'hdfs:///user/rn185083/northwind/orders_status'
;

CREATE EXTERNAL TABLE stg_northwind_orders (
  DTL__CAPXTIMESTAMP STRING,
  DTL__CAPXACTION STRING,
  DTL__CAPXROWID INT,
  id INT ,
  employee_id INT,
  customer_id INT,
  order_date TIMESTAMP,
  shipped_date TIMESTAMP,
  shipper_id INT,
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
  notes STRING,
  tax_rate DOUBLE,
  tax_status_id INT,
  status_id INT  
)
partitioned by (EDL_INGEST_CHANNEL STRING, EDL_INGEST_TIME STRING)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\u001F'
STORED AS TEXTFILE
LOCATION 'hdfs:///user/rn185083/northwind/orders'
;

CREATE EXTERNAL TABLE stg_northwind_products (
  DTL__CAPXTIMESTAMP STRING,
  DTL__CAPXACTION STRING,
  DTL__CAPXROWID INT,
  supplier_ids STRING,
  id INT,
  product_code STRING,
  product_name STRING,
  description STRING,
  standard_cost DECIMAL(19,4),
  list_price DECIMAL(19,4),
  reorder_level INT,
  target_level INT,
  quantity_per_unit STRING,
  discontinued INT,
  minimum_reorder_quantity INT,
  category STRING,
  attachments STRING  
)
partitioned by (EDL_INGEST_CHANNEL STRING, EDL_INGEST_TIME STRING)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\u001F'
STORED AS TEXTFILE
LOCATION 'hdfs:///user/rn185083/northwind/products'
;

CREATE EXTERNAL TABLE stg_northwind_purchase_order_status (
  DTL__CAPXTIMESTAMP STRING,
  DTL__CAPXACTION STRING,
  DTL__CAPXROWID INT,
  id INT,
  status STRING  
)
partitioned by (EDL_INGEST_CHANNEL STRING, EDL_INGEST_TIME STRING)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\u001F'
STORED AS TEXTFILE
LOCATION 'hdfs:///user/rn185083/northwind/purchase_order_status'
;

CREATE EXTERNAL TABLE stg_northwind_suppliers (
  DTL__CAPXTIMESTAMP STRING,
  DTL__CAPXACTION STRING,
  DTL__CAPXROWID INT,
  id INT ,
  company STRING,
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
  attachments STRING  
)
partitioned by (EDL_INGEST_CHANNEL STRING, EDL_INGEST_TIME STRING)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\u001F'
STORED AS TEXTFILE
LOCATION 'hdfs:///user/rn185083/northwind/suppliers'
;

CREATE EXTERNAL TABLE stg_northwind_purchase_orders (
  DTL__CAPXTIMESTAMP STRING,
  DTL__CAPXACTION STRING,
  DTL__CAPXROWID INT,
  id INT ,
  supplier_id INT,
  created_by INT,
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
  approved_by INT,
  approved_date TIMESTAMP,
  submitted_by INT  
)
partitioned by (EDL_INGEST_CHANNEL STRING, EDL_INGEST_TIME STRING)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\u001F'
STORED AS TEXTFILE
LOCATION 'hdfs:///user/rn185083/northwind/purchase_orders'
;

CREATE EXTERNAL TABLE stg_northwind_inventory_transactions (
  DTL__CAPXTIMESTAMP STRING,
  DTL__CAPXACTION STRING,
  DTL__CAPXROWID INT,
  id INT ,
  transaction_type INT,
  transaction_created_date TIMESTAMP,
  transaction_modified_date TIMESTAMP,
  product_id INT,
  quantity INT,
  purchase_order_id INT,
  customer_order_id INT,
  comments STRING  
)
partitioned by (EDL_INGEST_CHANNEL STRING, EDL_INGEST_TIME STRING)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\u001F'
STORED AS TEXTFILE
LOCATION 'hdfs:///user/rn185083/northwind/inventory_transactions'
;

CREATE EXTERNAL TABLE stg_northwind_invoices (
  DTL__CAPXTIMESTAMP STRING,
  DTL__CAPXACTION STRING,
  DTL__CAPXROWID INT,
  id INT ,
  order_id INT,
  invoice_date TIMESTAMP,
  due_date TIMESTAMP,
  tax DECIMAL(19,4),
  shipping DECIMAL(19,4),
  amount_due DECIMAL(19,4)  
)
partitioned by (EDL_INGEST_CHANNEL STRING, EDL_INGEST_TIME STRING)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\u001F'
STORED AS TEXTFILE
LOCATION 'hdfs:///user/rn185083/northwind/invoices'
;

CREATE EXTERNAL TABLE stg_northwind_order_details_status (
  DTL__CAPXTIMESTAMP STRING,
  DTL__CAPXACTION STRING,
  DTL__CAPXROWID INT,
  id INT,
  status_name STRING  
)
partitioned by (EDL_INGEST_CHANNEL STRING, EDL_INGEST_TIME STRING)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\u001F'
STORED AS TEXTFILE
LOCATION 'hdfs:///user/rn185083/northwind/order_details_status'
;

CREATE EXTERNAL TABLE stg_northwind_order_details (
  DTL__CAPXTIMESTAMP STRING,
  DTL__CAPXACTION STRING,
  DTL__CAPXROWID INT,
  id INT ,
  order_id INT,
  product_id INT,
  quantity DECIMAL(18,4),
  unit_price DECIMAL(19,4),
  discount DOUBLE,
  status_id INT,
  date_allocated TIMESTAMP,
  purchase_order_id INT,
  inventory_id INT  
)
partitioned by (EDL_INGEST_CHANNEL STRING, EDL_INGEST_TIME STRING)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\u001F'
STORED AS TEXTFILE
LOCATION 'hdfs:///user/rn185083/northwind/order_details'
;

CREATE EXTERNAL TABLE stg_northwind_purchase_order_details (
  DTL__CAPXTIMESTAMP STRING,
  DTL__CAPXACTION STRING,
  DTL__CAPXROWID INT,
  id INT ,
  purchase_order_id INT,
  product_id INT,
  quantity DECIMAL(18,4),
  unit_cost DECIMAL(19,4),
  date_received TIMESTAMP,
  posted_to_inventory INT,
  inventory_id INT  
)
partitioned by (EDL_INGEST_CHANNEL STRING, EDL_INGEST_TIME STRING)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\u001F'
STORED AS TEXTFILE
LOCATION 'hdfs:///user/rn185083/northwind/purchase_order_details'
;

CREATE EXTERNAL TABLE stg_northwind_sales_reports (
  DTL__CAPXTIMESTAMP STRING,
  DTL__CAPXACTION STRING,
  DTL__CAPXROWID INT,
  group_by STRING,
  display STRING,
  title STRING,
  filter_row_source STRING,
  default INT  
)
partitioned by (EDL_INGEST_CHANNEL STRING, EDL_INGEST_TIME STRING)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\u001F'
STORED AS TEXTFILE
LOCATION 'hdfs:///user/rn185083/northwind/sales_reports'
;

CREATE EXTERNAL TABLE stg_northwind_strings (
  DTL__CAPXTIMESTAMP STRING,
  DTL__CAPXACTION STRING,
  DTL__CAPXROWID INT,
  string_id INT ,
  string_data STRING  
)
partitioned by (EDL_INGEST_CHANNEL STRING, EDL_INGEST_TIME STRING)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\u001F'
STORED AS TEXTFILE
LOCATION 'hdfs:///user/rn185083/northwind/strings'
;
  

  


