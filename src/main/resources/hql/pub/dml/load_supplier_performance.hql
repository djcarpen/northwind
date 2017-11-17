insert overwrite table ${hivevar:targetDbName}.supplier_performance
select hs.company, hp.product_name, sum(spod.quantity) as item_count
from ${hivevar:sourceDbName}.s_purchase_orders spo
join ${hivevar:sourceDbName}.l_purchase_order_details lpod on lpod.purchase_orders_key = spo.purchase_orders_key
join ${hivevar:sourceDbName}.s_purchase_order_details spod on lpod.purchase_order_details_key = spod.purchase_order_details_key
join ${hivevar:sourceDbName}.l_purchase_orders lpo on spo.purchase_orders_key = lpo.purchase_orders_key
join ${hivevar:sourceDbName}.h_suppliers hs on lpo.suppliers_key = hs.suppliers_key
join ${hivevar:sourceDbName}.h_products hp on hp.products_key = lpod.products_key
group by hs.company, hp.product_name;
