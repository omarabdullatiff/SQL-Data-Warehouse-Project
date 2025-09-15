-- =============================================================================
-- Create Dimension: gold.dim_customers
-- =============================================================================

IF OBJECT_ID('gold.dim_customers' ,'V') is NOT NULL 
	DROP VIEW gold.dim_customers ;

CREATE VIEW gold.dim_customers 
AS
select 
	ROW_NUMBER() over(order by ci.cst_id) as customer_key ,
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
