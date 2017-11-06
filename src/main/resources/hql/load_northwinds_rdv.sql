
-- seed 1900-01-01-00
-- feed 2017-01-01-13
-- feed 2017-01-01-19



insert into table H_customers
select distinct 
        upper(concat_ws("-",regexp_replace(last_name, '"', ''), regexp_replace(first_name, '"', ''), regexp_replace(company, '"', ''))) customers_key,
        id,
        regexp_replace(company, '"', '') company,
        current_timestamp 
from stg_northwind_customers sc
where not exists (select 1 from h_customers hc where upper(concat_ws("-",regexp_replace(sc.last_name, '"', ''), regexp_replace(sc.first_name, '"', ''), regexp_replace(sc.company, '"', ''))) = hc.customers_key)
and edl_ingest_time = ${edl_ingest_time||||string}$;
 
insert into table H_employees
select distinct
        upper(regexp_replace(email_address, '"', '')),
        id,
        regexp_replace(email_address, '"', ''),
        current_timestamp 
from stg_northwind_employees se
where not exists (select 1 from h_employees he where upper(regexp_replace(se.email_address, '"', '')) = he.employees_key)
and edl_ingest_time = ${edl_ingest_time||||string}$;

INSERT into table H_privileges
select distinct
        upper(regexp_replace(privilege_name, '"', '')),
        id,
        regexp_replace(privilege_name, '"', ''),
        current_timestamp 
from stg_northwind_privileges sp
where not exists (select 1 from h_privileges hp where upper(regexp_replace(sp.privilege_name, '"', '')) = hp.privileges_key)
and edl_ingest_time = ${edl_ingest_time||||string}$;

insert into table H_shippers 
select distinct
        upper(concat_ws("-",regexp_replace(last_name, '"', ''), regexp_replace(first_name, '"', ''), regexp_replace(company, '"', ''))),
        id,
        regexp_replace(company, '"', ''),
        current_timestamp 
from stg_northwind_shippers ss
where not exists (select 1 from h_shippers hs where upper(concat_ws("-",regexp_replace(ss.last_name, '"', ''), regexp_replace(ss.first_name, '"', ''), regexp_replace(ss.company, '"', ''))) = hs.shippers_key)
and edl_ingest_time = ${edl_ingest_time||||string}$;

insert into table H_products 
select distinct
        upper(regexp_replace(sp.product_code, '"', '')),
        sp.id,
        regexp_replace(sp.product_name, '"', ''),
        current_timestamp 
from stg_northwind_products sp
where not exists (select 1 from h_products hp where upper(regexp_replace(sp.product_code, '"', '')) = hp.products_key)
and edl_ingest_time = ${edl_ingest_time||||string}$;

insert into table H_invoices
select
        upper(cast(si.id as string)) invoices_key,
        si.id invoice_id,
        CURRENT_TIMESTAMP() load_dt
from stg_northwind_invoices si
where not exists (select 1 from h_invoices hi where upper(cast(si.id as string)) = hi.invoices_key)
and edl_ingest_time = ${edl_ingest_time||||string}$;

insert into table H_orders 
select distinct
        cast(so.id as string) orders_key,
        so.id order_id,
        CURRENT_TIMESTAMP() load_dt
from stg_northwind_orders so
where not exists (select 1 from h_orders ho where cast(so.id as string) = ho.orders_key)
and edl_ingest_time = ${edl_ingest_time||||string}$;

insert into table H_order_details 
select 
    upper(cast(sod.id as string)) order_details_key,
    sod.id order_details_id,
    CURRENT_TIMESTAMP() load_dt
FROM stg_northwind_order_details sod
where not exists (select 1 from h_order_details hod where upper(cast(sod.id as string)) = hod.order_details_key)
and edl_ingest_time = ${edl_ingest_time||||string}$;

insert into table h_purchase_orders
select distinct
    upper(cast(id as string)) purchase_orders_key,
    spo.id purchase_order_id,
    CURRENT_TIMESTAMP() load_dt
FROM stg_northwind_purchase_orders spo
where not exists (select 1 from h_purchase_orders hpo where upper(cast(spo.id as string)) = hpo.purchase_orders_key)
and edl_ingest_time = ${edl_ingest_time||||string}$;

insert into table h_purchase_order_details
select distinct 
    upper(cast(spod.id as string)) purchase_order_details_key,
    spod.id purchase_order_details_id,
    CURRENT_TIMESTAMP() load_dt
FROM stg_northwind_purchase_order_details spod
where not exists (select 1 from h_purchase_order_details hpod where upper(cast(spod.id as string)) = hpod.purchase_order_details_key)
and edl_ingest_time = ${edl_ingest_time||||string}$;

insert into table h_suppliers 
select distinct
        upper(concat_ws("-",cast(id as string), trim(regexp_replace(company,'"','')))) suppliers_key,
        id supplier_id,
        trim(regexp_replace(company,'"','')),
        CURRENT_TIMESTAMP() load_dt
from stg_northwind_suppliers ss
where not exists (select 1 from h_suppliers hs where upper(concat_ws("-",cast(ss.id as string), trim(regexp_replace(ss.company,'"','')))) = hs.suppliers_key)
and edl_ingest_time = ${edl_ingest_time||||string}$;

insert into table h_inventory_transactions
select distinct 
    upper(cast(id as string)) inventory_transactions_key,
    CURRENT_TIMESTAMP() load_dt,
    null load_end_dt
from stg_northwind_inventory_transactions sit
where not exists (select 1 from h_inventory_transactions hit where upper(cast(sit.id as string)) = hit.inventory_transactions_key)
and edl_ingest_time = ${edl_ingest_time||||string}$;
 
insert into table S_customers
select
        upper(concat_ws("-",regexp_replace(last_name, '"', ''), regexp_replace(first_name, '"', ''), regexp_replace(company, '"', ''))),
        current_timestamp,
        unix_timestamp(edl_ingest_time,'yyyy-MM-dd-hh'),
        case when regexp_replace(dtl__capxtimestamp,'"','') = 'D' then 1 else 0 end deleted,
        regexp_replace(last_name, '"', ''),
        regexp_replace(first_name, '"', ''),
        regexp_replace(email_address, '"', ''),
        regexp_replace(job_title, '"', ''),
        regexp_replace(business_phone, '"', ''),
        regexp_replace(home_phone, '"', ''),
        regexp_replace(mobile_phone, '"', ''),
        regexp_replace(fax_number, '"', ''),
        regexp_replace(address, '"', ''),
        regexp_replace(city, '"', ''),
        regexp_replace(state_province, '"', ''),
        regexp_replace(zip_postal_code, '"', ''),
        regexp_replace(country_region, '"', ''),
        regexp_replace(web_page, '"', ''),
        regexp_replace(notes, '"', ''),
        regexp_replace(attachments, '"', '') 
from stg_northwind_customers
where edl_ingest_time = ${edl_ingest_time||||string}$;
 
insert into table S_employees
select
        upper(regexp_replace(email_address, '"', '')),
        current_timestamp,
        unix_timestamp(edl_ingest_time,'yyyy-MM-dd-hh'),
        case when regexp_replace(dtl__capxtimestamp,'"','') = 'D' then 1 else 0 end deleted,
        regexp_replace(company, '"', ''),
        regexp_replace(last_name, '"', ''),
        regexp_replace(first_name, '"', ''),
        regexp_replace(job_title, '"', ''),
        regexp_replace(business_phone, '"', ''),
        regexp_replace(home_phone, '"', ''),
        regexp_replace(mobile_phone, '"', ''),
        regexp_replace(fax_number, '"', ''),
        regexp_replace(address, '"', ''),
        regexp_replace(city, '"', ''),
        regexp_replace(state_province, '"', ''),
        regexp_replace(zip_postal_code, '"', ''),
        regexp_replace(country_region, '"', ''),
        regexp_replace(web_page, '"', ''),
        regexp_replace(notes, '"', ''),
        regexp_replace(attachments, '"', '') 
from stg_northwind_employees
where edl_ingest_time = ${edl_ingest_time||||string}$;
 
insert into table S_shippers
select
        upper(concat_ws("-",regexp_replace(last_name, '"', ''), regexp_replace(first_name, '"', ''), regexp_replace(company, '"', ''))),
        current_timestamp,
        unix_timestamp(edl_ingest_time,'yyyy-MM-dd-hh'),
        case when regexp_replace(dtl__capxtimestamp,'"','') = 'D' then 1 else 0 end deleted,
        regexp_replace(last_name, '"', ''),
        regexp_replace(first_name, '"', ''),
        regexp_replace(email_address, '"', ''),
        regexp_replace(job_title, '"', ''),
        regexp_replace(business_phone, '"', ''),
        regexp_replace(home_phone, '"', ''),
        regexp_replace(mobile_phone, '"', ''),
        regexp_replace(fax_number, '"', ''),
        regexp_replace(address, '"', ''),
        regexp_replace(city, '"', ''),
        regexp_replace(state_province, '"', ''),
        regexp_replace(zip_postal_code, '"', ''),
        regexp_replace(country_region, '"', ''),
        regexp_replace(web_page, '"', ''),
        regexp_replace(notes, '"', ''),
        regexp_replace(attachments, '"', '') 
from stg_northwind_shippers
where edl_ingest_time = ${edl_ingest_time||||string}$;
 
insert into table S_products
select 
        upper(regexp_replace(product_code, '"', '')),
        current_timestamp,
        unix_timestamp(edl_ingest_time,'yyyy-MM-dd-hh'),
        case when regexp_replace(dtl__capxtimestamp,'"','') = 'D' then 1 else 0 end deleted,
        regexp_replace(product_code, '"', ''),
        regexp_replace(description, '"', ''),
        standard_cost,
        list_price,
        reorder_level,
        target_level,
        regexp_replace(quantity_per_unit, '"', ''),
        discontinued,
        minimum_reorder_quantity,
        regexp_replace(category, '"', ''),
        regexp_replace(attachments, '"', '') 
from stg_northwind_products
where edl_ingest_time = ${edl_ingest_time||||string}$;

insert into table S_invoices
select
        upper(cast(id as string)) invoices_key,
        unix_timestamp(edl_ingest_time,'yyyy-MM-dd-hh'),
        CURRENT_TIMESTAMP() load_dt,
        case when regexp_replace(dtl__capxtimestamp,'"','') = 'D' then 1 else 0 end deleted,
        invoice_date,
        due_date,
        tax,
        shipping,
        amount_due
from stg_northwind_invoices
where edl_ingest_time = ${edl_ingest_time||||string}$;
         
insert into table s_orders
SELECT
    upper(cast(id as string)) orders_key,
    CURRENT_TIMESTAMP() load_dt,
    unix_timestamp(edl_ingest_time,'yyyy-MM-dd-hh'),
    case when regexp_replace(dtl__capxtimestamp,'"','') = 'D' then 1 else 0 end deleted,  
    tax_status_id,
    status_id,
    order_date,
    shipped_date,
    trim(regexp_replace(ship_name,'"','')) ship_name,
    trim(regexp_replace(ship_address,'"','')) ship_address,
    trim(regexp_replace(ship_city,'"','')) ship_city,
    trim(regexp_replace(ship_state_province,'"','')) ship_state_province,
    trim(regexp_replace(ship_zip_postal_code,'"','')) ship_zip_postal_code,
    trim(regexp_replace(ship_country_region,'"','')) ship_country_region,
    trim(regexp_replace(shipping_fee,'"','')) shipping_fee,
    taxes,
    trim(regexp_replace(payment_type,'"','')) payment_type,
    paid_date,
    trim(regexp_replace(notes,'"','')) notes
FROM stg_northwind_orders
where edl_ingest_time = ${edl_ingest_time||||string}$;

insert into table  S_order_details 
select
        upper(cast(id as string)) order_details_key,
        CURRENT_TIMESTAMP() load_dt,
        unix_timestamp(edl_ingest_time,'yyyy-MM-dd-hh'),
        case when regexp_replace(dtl__capxtimestamp,'"','') = 'D' then 1 else 0 end deleted,
        quantity,
        unit_price,
        discount,
        status_id,
        date_allocated
from stg_northwind_order_details
where edl_ingest_time = ${edl_ingest_time||||string}$;

insert into table s_purchase_orders
SELECT
        upper(cast(id as string)) purchase_orders_key,
        CURRENT_TIMESTAMP() load_dt,
        unix_timestamp(edl_ingest_time,'yyyy-MM-dd-hh'),
        case when regexp_replace(dtl__capxtimestamp,'"','') = 'D' then 1 else 0 end deleted,
        status_id,
        submitted_date,
        creation_date,
        expected_date,
        shipping_fee,
        taxes,
        payment_date,
        payment_amount,
        trim(regexp_replace(payment_method,'"','')) payment_method,
        trim(regexp_replace(notes,'"','')) notes,
        approved_date
FROM stg_northwind_purchase_orders
where edl_ingest_time = ${edl_ingest_time||||string}$;

insert into table S_purchase_order_details 
select 
        upper(cast(id as string)) purchase_order_details_key,
        CURRENT_TIMESTAMP() load_dt,
        unix_timestamp(edl_ingest_time,'yyyy-MM-dd-hh'),
        case when regexp_replace(dtl__capxtimestamp,'"','') = 'D' then 1 else 0 end deleted,
        quantity,
        unit_cost,
        date_received,
        posted_to_inventory
FROM stg_northwind_purchase_order_details
where edl_ingest_time = ${edl_ingest_time||||string}$;
 
insert into table s_suppliers
SELECT
        upper(concat_ws("-",cast(id as string), trim(regexp_replace(company,'"','')))) suppliers_key,
        CURRENT_TIMESTAMP() load_dt,
        unix_timestamp(edl_ingest_time,'yyyy-MM-dd-hh'),
        case when regexp_replace(dtl__capxtimestamp,'"','') = 'D' then 1 else 0 end deleted,
        trim(regexp_replace(last_name,'"','')) last_name,
        trim(regexp_replace(first_name,'"','')) first_name,
        trim(regexp_replace(email_address,'"','')) email_address,
        trim(regexp_replace(job_title,'"','')) job_title,
        trim(regexp_replace(business_phone,'"','')) business_phone,
        trim(regexp_replace(home_phone,'"','')) home_phone,
        trim(regexp_replace(mobile_phone,'"','')) mobile_phone,
        trim(regexp_replace(fax_number,'"','')) fax_number,
        trim(regexp_replace(address,'"','')) address,
        trim(regexp_replace(city,'"','')) city,
        trim(regexp_replace(state_province,'"','')) state_province,
        trim(regexp_replace(zip_postal_code,'"','')) zip_postal_code,
        trim(regexp_replace(country_region,'"','')) country_region,
        trim(regexp_replace(web_page,'"','')) web_page,
        trim(regexp_replace(notes,'"','')) notes,
        trim(regexp_replace(attachments,'"','')) attachments
FROM stg_northwind_suppliers
where edl_ingest_time = ${edl_ingest_time||||string}$;

insert into table s_inventory_transactions
select 
        upper(cast(id as string)) inventory_transactions_key,
        CURRENT_TIMESTAMP() load_dt,
        unix_timestamp(edl_ingest_time,'yyyy-MM-dd-hh'),
        case when regexp_replace(dtl__capxtimestamp,'"','') = 'D' then 1 else 0 end deleted,
        transaction_type,
        transaction_created_date,
        transaction_modified_date,
        quantity,
        trim(regexp_replace(comments,'"','')) comments
from stg_northwind_inventory_transactions
where edl_ingest_time = ${edl_ingest_time||||string}$;
 
insert into L_employee_privileges 
select distinct
        upper(concat_ws("-",regexp_replace(nvl(e.email_address,''), '"', ''), regexp_replace(nvl(p.privilege_name,''), '"', ''))) link_employee_privileges_key,
        upper(regexp_replace(nvl(e.email_address,''), '"', '')) employees_key,
        upper(regexp_replace(nvl(p.privilege_name,''), '"', '')) privileges_key,
        CURRENT_TIMESTAMP() load_dt,
        unix_timestamp(edl_ingest_time,'yyyy-MM-dd-hh')
from stg_northwind_employee_privileges ep
join (select distinct id, email_address from stg_northwind_employees) e on ep.employee_id = e.id
join (select distinct id, privilege_name from stg_northwind_privileges) p on ep.privilege_id = p.id
where edl_ingest_time = ${edl_ingest_time||||string}$
and not exists (select 1 from l_employee_privileges lep where lep.link_employee_privileges_key = upper(concat_ws("-",regexp_replace(nvl(e.email_address,''), '"', ''), regexp_replace(nvl(p.privilege_name,''), '"', ''))));

insert into table l_invoices
select distinct
        upper(concat_ws("-",regexp_replace(nvl(cast(i.id as string),''), '"', ''), regexp_replace(nvl(cast(i.order_id as string),''), '"', ''))) link_invoices_key,
        upper(nvl(cast(i.id as string),'')) invoices_key,
        upper(nvl(cast(i.order_id as string),'')) orders_key,
        CURRENT_TIMESTAMP() load_dt,
        unix_timestamp(edl_ingest_time,'yyyy-MM-dd-hh')
from stg_northwind_invoices i
where edl_ingest_time = ${edl_ingest_time||||string}$
and not exists (select 1 from l_invoices lo where link_invoices_key = upper(concat_ws("-",regexp_replace(nvl(cast(i.id as string),''), '"', ''), regexp_replace(nvl(cast(i.order_id as string),''), '"', ''))));    

insert into table L_purchase_orders 
select distinct
        upper(concat_ws("-",nvl(cast(po.id as string),''),nvl(cast(s.id as string),''), trim(regexp_replace(nvl(s.company,''),'"','')),regexp_replace(nvl(e1.email_address,''), '"', ''), regexp_replace(nvl(e2.email_address,''), '"', ''),regexp_replace(nvl(e3.email_address,''), '"', ''))) link_purchase_orders_key,
        upper(nvl(cast(po.id as string),'')) purchase_orders_key,
        upper(concat_ws("-",nvl(cast(s.id as string),''), trim(regexp_replace(nvl(s.company,''),'"','')))) suppliers_key,
        upper(regexp_replace(nvl(e1.email_address,''), '"', '')) created_by_employees_key,
        upper(regexp_replace(nvl(e2.email_address,''), '"', '')) approved_by_employees_key,
        upper(regexp_replace(nvl(e3.email_address,''), '"', '')) submitted_by_employees_key,
        CURRENT_TIMESTAMP() load_dt,
        unix_timestamp(po.edl_ingest_time,'yyyy-MM-dd-hh')
from stg_northwind_purchase_orders po
left join stg_northwind_suppliers s on po.supplier_id = s.id
left join stg_northwind_employees e1 on po.created_by = e1.id
left join stg_northwind_employees e2 on po.approved_by = e2.id
left join stg_northwind_employees e3 on po.submitted_by = e3.id
where po.edl_ingest_time = ${edl_ingest_time||||string}$
and not exists (select 1 from l_purchase_orders lpo where link_purchase_orders_key = upper(concat_ws("-",nvl(cast(po.id as string),''),nvl(cast(s.id as string),''), trim(regexp_replace(nvl(s.company,''),'"','')),regexp_replace(nvl(e1.email_address,''), '"', ''), regexp_replace(nvl(e2.email_address,''), '"', ''),regexp_replace(nvl(e3.email_address,''), '"', ''))));
            
insert into table l_orders
select distinct
        upper(concat_ws("-",nvl(cast(o.id as string),''),regexp_replace(nvl(e.email_address,''), '"', ''),regexp_replace(nvl(c.last_name,''), '"', ''), regexp_replace(nvl(c.first_name,''), '"', ''), regexp_replace(nvl(c.company,''), '"', ''),regexp_replace(nvl(sh.last_name,''), '"', ''), regexp_replace(nvl(sh.first_name,''), '"', ''), regexp_replace(nvl(sh.company,''), '"', ''))) link_orders_key,
        upper(nvl(cast(o.id as string),'')) orders_key,
        upper(regexp_replace(nvl(e.email_address,''), '"', '')) employees_key,
        upper(concat_ws("-",regexp_replace(nvl(c.last_name,''), '"', ''), regexp_replace(nvl(c.first_name,''), '"', ''), regexp_replace(nvl(c.company,''), '"', ''))) customers_key,
        upper(concat_ws("-",regexp_replace(nvl(sh.last_name,''), '"', ''), regexp_replace(nvl(sh.first_name,''), '"', ''), regexp_replace(nvl(sh.company,''), '"', ''))) shippers_key,
        CURRENT_TIMESTAMP() load_dt,
        unix_timestamp(o.edl_ingest_time,'yyyy-MM-dd-hh')
from stg_northwind_orders o
left join stg_northwind_employees e on o.employee_id = e.id
left join stg_northwind_customers c on o.customer_id = c.id
left join stg_northwind_shippers sh on o.shipper_id = sh.id
where o.edl_ingest_time = ${edl_ingest_time||||string}$
and not exists (select 1 from l_orders where link_orders_key = upper(concat_ws("-",nvl(cast(o.id as string),''),regexp_replace(nvl(e.email_address,''), '"', ''),regexp_replace(nvl(c.last_name,''), '"', ''), regexp_replace(nvl(c.first_name,''), '"', ''), regexp_replace(nvl(c.company,''), '"', ''),regexp_replace(nvl(sh.last_name,''), '"', ''), regexp_replace(nvl(sh.first_name,''), '"', ''), regexp_replace(nvl(sh.company,''), '"', ''))));

insert into table l_order_details  
select distinct
        upper(concat_ws("-",nvl(cast(od.id as string),''),nvl(cast(od.order_id as string),''),regexp_replace(nvl(p.product_code,''), '"', ''),nvl(cast(od.purchase_order_id as string),''),nvl(cast(od.inventory_id as string),''))) link_order_details_key,
        upper(nvl(cast(od.id as string),'')) order_details_key,
        upper(nvl(cast(od.order_id as string),'')) orders_key,
        upper(regexp_replace(nvl(p.product_code,''), '"', '')) products_key,
        upper(nvl(cast(od.purchase_order_id as string),'')) purchase_orders_key,
        upper(nvl(cast(od.inventory_id as string),'')) inventory_transactions_key,
        CURRENT_TIMESTAMP() load_dt,
        unix_timestamp(od.edl_ingest_time,'yyyy-MM-dd-hh')
from stg_northwind_order_details od
left join stg_northwind_products p on od.product_id = p.id
where od.edl_ingest_time = ${edl_ingest_time||||string}$
and not exists (select 1 from l_order_details lod where lod.link_order_details_key = upper(concat_ws("-",nvl(cast(od.id as string),''),nvl(cast(od.order_id as string),''),regexp_replace(nvl(p.product_code,''), '"', ''),nvl(cast(od.purchase_order_id as string),''),nvl(cast(od.inventory_id as string),''))));

insert into table l_purchase_order_details
select distinct
        upper(concat_ws("-",nvl(cast(pod.id as string),''),nvl(cast(pod.purchase_order_id as string),''),nvl(cast(pod.inventory_id as string),''))) link_purchase_order_details_key,
        upper(nvl(cast(pod.id as string),'')) purchase_order_details_key,
        upper(nvl(cast(pod.purchase_order_id as string),'')) purchase_orders_key,
        upper(nvl(cast(pod.inventory_id as string),'')) inventory_transactions_key,
        CURRENT_TIMESTAMP() load_dt,
        unix_timestamp(edl_ingest_time,'yyyy-MM-dd-hh')
from stg_northwind_purchase_order_details pod
where edl_ingest_time = ${edl_ingest_time||||string}$
and not exists (select 1 from l_purchase_order_details where link_purchase_order_details_key = upper(concat_ws("-",nvl(cast(pod.id as string),''),nvl(cast(pod.purchase_order_id as string),''),nvl(cast(pod.inventory_id as string),''))));
 
insert into table R_purchase_order_status 
select distinct
        id purchase_order_status_id,
        regexp_replace(status,'"','') status,
        CURRENT_TIMESTAMP() load_dt,
        unix_timestamp(edl_ingest_time,'yyyy-MM-dd-hh')
from stg_northwind_purchase_order_status spos
where not exists (select 1 from r_purchase_order_status rpos where spos.id = rpos.purchase_order_status_id)
and edl_ingest_time = ${edl_ingest_time||||string}$;

insert into table R_inventory_transaction_types 
select distinct
        id inventory_transaction_type_id,
        regexp_replace(type_name,'"','') type_name,
        CURRENT_TIMESTAMP() load_dt,
        unix_timestamp(sitt.edl_ingest_time,'yyyy-MM-dd-hh')
from stg_northwind_inventory_transaction_types sitt
where not exists (select 1 from r_inventory_transaction_types ritt where sitt.id = ritt.inventory_transaction_type_id)
and edl_ingest_time = ${edl_ingest_time||||string}$;

insert into table R_order_details_status 
select distinct
        id order_details_status_id,
        regexp_replace(status_name,'"','') status_name,
        CURRENT_TIMESTAMP() load_dt,
        unix_timestamp(edl_ingest_time,'yyyy-MM-dd-hh')
from stg_northwind_order_details_status nods
where not exists (select 1 from r_order_details_status rods where nods.id = rods.order_details_status_id)
and edl_ingest_time = ${edl_ingest_time||||string}$;

insert into table R_orders_tax_status
select distinct
        id order_tax_status_id,
        regexp_replace(tax_status_name,'"','') tax_status_name,
        CURRENT_TIMESTAMP() load_dt,
        unix_timestamp(edl_ingest_time,'yyyy-MM-dd-hh')
from stg_northwind_orders_tax_status sots
where not exists (select 1 from r_orders_tax_status rots where sots.id = rots.order_tax_status_id)
and edl_ingest_time = ${edl_ingest_time||||string}$;

insert into table R_orders_status 
select distinct
        id order_status_id,
        regexp_replace(status_name,'"','') status_name,
        CURRENT_TIMESTAMP() load_dt,
        unix_timestamp(edl_ingest_time,'yyyy-MM-dd-hh')
from stg_northwind_orders_status sos
where not exists (select 1 from r_orders_status ros where sos.id = ros.order_status_id)
and edl_ingest_time = ${edl_ingest_time||||string}$;

insert into table R_sales_reports 
select 
        regexp_replace(group_by,'"','') group_by,
        regexp_replace(display,'"','') display,
        regexp_replace(title,'"','') title,
        regexp_replace(filter_row_source,'"','') filter_row_source,
        `default`,
        CURRENT_TIMESTAMP() load_dt,
        unix_timestamp(ssr.edl_ingest_time,'yyyy-MM-dd-hh')
from stg_northwind_sales_reports ssr
where not exists (
        select 1 from r_sales_reports rsr 
        where regexp_replace(ssr.group_by,'"','') = rsr.group_by
                and regexp_replace(ssr.display,'"','') = rsr.display
                and regexp_replace(ssr.title,'"','') = rsr.title
                and regexp_replace(ssr.filter_row_source,'"','') = rsr.filter_row_source
                and ssr.`default` = rsr.`default`)
and edl_ingest_time = ${edl_ingest_time||||string}$;

insert into table R_strings 
select
          string_id,
          regexp_replace(string_data,'"','') string_data,
          CURRENT_TIMESTAMP() load_dt,
          unix_timestamp(ss.edl_ingest_time,'yyyy-MM-dd-hh')
from stg_northwind_strings ss
where not exists (select 1 from r_strings rs where ss.string_id = rs.string_id)
and edl_ingest_time = ${edl_ingest_time||||string}$;