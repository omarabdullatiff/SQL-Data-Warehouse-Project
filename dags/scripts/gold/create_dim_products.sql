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
