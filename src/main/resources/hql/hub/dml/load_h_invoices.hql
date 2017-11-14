insert into table ${hivevar:targetDbName}.h_invoices
select 
    invoices_key,
    invoice_id,
    load_dt fromh_invoices
from $(hivevar:sourceDbName).h_invoices raw
where not exists
    ( select 1
      from ${hivevar:targetDbName}.h_invoices hub
      where raw.invoices_key = hub.invoices_key) ;