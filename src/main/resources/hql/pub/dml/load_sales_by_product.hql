insert overwrite table ${hivevar:targetDbName}.sales_by_product
select product_name, count(so.orders_key) order_count, sum(sod.quantity) quantity, sum(sod.unit_price) amount
from ${hivevar:sourceDbName}.s_order_details sod
join ${hivevar:sourceDbName}.l_order_details lod on sod.order_details_key = lod.order_details_key
join ${hivevar:sourceDbName}.s_orders so on lod.orders_key = so.orders_key and so.order_date is not null
join ${hivevar:sourceDbName}.l_orders lo on so.orders_key = lo.orders_key
join ${hivevar:sourceDbName}.h_products hp on lod.products_key = hp.products_key
group by product_name;
