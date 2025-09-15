/*
this script to create views for gold layer 
These views can be queried directly for analytics and reportin
data model in star schema 
*/

-- =============================================================================
-- Create Dimension: gold.dim_customers
-- =============================================================================

IF OBJECT_ID('gold.dim_customers' ,'V') is NOT NULL 
	DROP VIEW gold.dim_customers ;

CREATE VIEW gold.dim_customers 
AS
select 
	ROW_NUMBER() over(order by cst_id) as customer_key ,
	ci.cst_id as customer_id,
	ci.cst_key as customer_number, 
	ci.cst_firstname as first_name,
	ci.cst_lastname as last_name, 
	cl.cntry as country ,
	ci.cst_marital_status as marital_status, 
	CASE 
		when ci.cst_gndr != 'N/A' then ci.cst_gndr 
		else coalesce(ca.gen , 'N/A')
	END as gender,
	ca.bdate as birthdate,
	ci.cst_create_date as create_date

from silver.crm_cust_info as ci 
LEFT JOIN  silver.erp_cust_az12 ca 
on ci.cst_key = ca.cid 
LEFT JOIN silver.erp_loc_a101 as cl 
on ci.cst_key = cl.cid ;

-- =============================================================================
-- Create Dimension: gold.dim_products
-- =============================================================================
IF OBJECT_ID('gold.dim_products' ,'V') is NOT NULL 
	DROP VIEW gold.dim_products ;

CREATE VIEW gold.dim_products
as
SELECT 
    ROW_NUMBER() over(order by p.prd_start_dt , p.prd_key ) as product_key ,
    p.prd_id as product_id,
    p.prd_key product_number,
    p.prd_nm as product_name,
    p.cat_id category_id,
    pc.cat as category , 
    pc.subcat as subcategory,
    pc.maintenance ,
    p.prd_cost as cost,
    p.prd_line as product_line,
    p.prd_start_dt as start_date
    
from silver.crm_prd_info as p 
LEFT join silver.erp_px_cat_g1v2 as pc 
on p.cat_id = pc.id

where p.prd_end_dt is null -- to remove historical data to simplifies queries and prevents double counting
;

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
on s.sls_prd_key = p.product_number
;




