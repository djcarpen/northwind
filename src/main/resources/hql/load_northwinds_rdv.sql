insert into table DR_northwind_rdv.H_invoices
select
  cast(id as string) invoices_key,
  id invoice_id,
  CURRENT_TIMESTAMP() load_dt
from justin_northwind.stg_northwind_invoices;

insert into table DR_northwind_rdv.S_invoices
select
  cast(id as string) invoices_key,
  CURRENT_TIMESTAMP() load_dt,
  null load_end_dt,
  invoice_date,
  due_date,
  tax,
  shipping,
  amount_due
from justin_northwind.stg_northwind_invoices;

insert into table DR_northwind_rdv.H_orders 
select
  cast(id as string) orders_key,
  id order_id,
  CURRENT_TIMESTAMP() load_dt
from justin_northwind.stg_northwind_orders;

insert into table DR_northwind_rdv.S_orders 
select 
  cast(o.id as string) orders_key,
  CURRENT_TIMESTAMP() load_dt,
  null load_end_dt,
  o.employee_id,
  o.customer_id,
  o.tax_status_id,
  o.status_id,
  o.shipper_id,
  o.order_date,
  o.shipped_date,
  o.ship_name,
  o.ship_address,
  o.ship_city,
  o.ship_state_province,
  o.ship_zip_postal_code,
  o.ship_country_region,
  o.shipping_fee,
  o.taxes,
  o.payment_type,
  o.paid_date,
  o.notes,
  o.tax_rate,
  od.status_id order_details_status_id,
  od.quantity,
  od.unit_price,
  od.discount,
  od.date_allocated
from justin_northwind.stg_northwind_orders o
join justin_northwind.stg_northwind_order_details od on od.order_id = o.id;