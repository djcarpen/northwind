drop table if exists ${hivevar:targetDbName}.sales_trend_by_month;
drop table if exists ${hivevar:targetDbName}.employee_performance_by_month;
drop table if exists ${hivevar:targetDbName}.sales_by_geography;
drop table if exists ${hivevar:targetDbName}.sales_by_customer;
drop table if exists ${hivevar:targetDbName}.sales_by_product;
drop table if exists ${hivevar:targetDbName}.shipper_performance;
drop table if exists ${hivevar:targetDbName}.supplier_performance;

create table ${hivevar:targetDbName}.sales_trend_by_month (
        `year` INT, 
        `month` INT, 
        amount DECIMAL(19,4)) STORED AS ORC;
        
create table ${hivevar:targetDbName}.employee_performance_by_month (
        employee_name string, 
        job_title string,
        unit_price DECIMAL(19,4)) STORED AS ORC;
        

create table ${hivevar:targetDbName}.sales_by_geography (
        `state` string, 
        `customer count` int, 
        quantity DECIMAL(18,4), 
        amount DECIMAL(19,4)) STORED AS ORC;      
        
create table ${hivevar:targetDbName}.sales_by_customer (
        company string,
        product_name string,
        order_count int,
        quantity DECIMAL(18,4),
        amount DECIMAL(19,4)) STORED AS ORC;   
  
create table ${hivevar:targetDbName}.sales_by_product ( 
        product_name string,
        order_count int,
        quantity DECIMAL(18,4),
        amount DECIMAL(19,4)) STORED AS ORC;  
  
create table ${hivevar:targetDbName}.shipper_performance (  
        company string, 
        delivery_count int, 
        not_delivered int, 
        avg_freight_cost DECIMAL(19,4)) STORED AS ORC;  

create table ${hivevar:targetDbName}.supplier_performance ( 
        company string,
        product_name string,
        item_count int) STORED AS ORC;
