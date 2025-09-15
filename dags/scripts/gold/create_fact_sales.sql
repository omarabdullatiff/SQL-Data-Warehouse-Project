-- =============================================================================
-- Create fact: gold.fact_sales
-- =============================================================================

if OBJECT_ID('gold.fact_sales' , 'V') is not null 
    DROP VIEW gold.fact_sales ; 

CREATE VIEW gold.fact_sales
as
SELECT 
    s.sls_ord_num as order_number,
    p.product_key as product_key,
    c.customer_key as customer_key,
    s.sls_order_dt as order_date,
    s.sls_ship_dt as ship_date,
    s.sls_due_dt as due_date ,
    s.sls_sales as sales_amount,
    s.sls_quantity as quantity,
    s.sls_price as price
from silver.crm_sales_details s 
LEFT JOIN  gold.dim_customers as c 
on s.sls_cust_id = c.customer_id
LEFT join gold.dim_products p
on s.sls_prd_key = p.product_number ;
