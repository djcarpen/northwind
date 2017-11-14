insert into table ${hivevar:targetDbName}.h_inventory_transactions
select 
    inventory_transactions_key,
    inventory_transaction_id,
    load_dt
from $(hivevar:sourceDbName).h_inventory_transactions raw
where not exists
    ( select 1
      from ${hivevar:targetDbName}.h_inventory_transactions hub
      where raw.inventory_transactions_key = hub.inventory_transactions_key) ;