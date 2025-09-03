TRUNCATE TABLE silver.crm_cust_info ;

INSERT into silver.crm_cust_info(
cst_id,
cst_key, 
cst_firstname,
cst_lastname ,
cst_marital_status,
cst_gndr,
cst_create_date
)
select  
cst_id,
cst_key,
TRIM(cst_firstname) AS cst_firstname ,
TRIM(cst_lastname) AS cst_lastname , 
CASE 
when UPPER(TRIM(cst_marital_status)) = 'M' then 'Married'
when UPPER(TRIM(cst_marital_status)) = 'S' then 'Single'
else 'N/A'
END AS cst_marital_status,
CASE 
when UPPER(TRIM(cst_gndr)) = 'M' then 'Male'
when UPPER(TRIM(cst_gndr)) = 'S' then 'Female'
else 'N/A'
END AS cst_gndr,
cst_create_date

from 
(select
 * , 
ROW_NUMBER() OVER( PARTITION BY cst_id ORDER BY cst_create_date DESC) as flag_last
from  bronze.crm_cust_info 
where cst_id is not NULL
) t
where flag_last = 1 
; 

TRUNCATE TABLE silver.crm_prd_info ;

INSERT into silver.crm_prd_info (
    prd_id ,
    cat_id ,
    prd_key ,
    prd_nm ,
    prd_cost ,
    prd_line ,
    prd_start_dt,
    prd_end_dt
)

select 
prd_id , 
REPLACE(SUBSTRING(prd_key , 1,5) , '-' ,'_') as cat_id ,
SUBSTRING(prd_key , 7,LEN(prd_key)) as prd_key,
prd_nm,
ISNULL(prd_cost , 0) as prd_cost ,
CASE
    when UPPER(TRIM(prd_line)) = 'R' then 'Road'
    when UPPER(TRIM(prd_line)) = 'M' then 'Mountain'
    when UPPER(TRIM(prd_line)) = 'S' then 'Other Sales'
    when UPPER(TRIM(prd_line)) = 'T' then 'Touring'
    else 'N/A'

END as prd_line , 
CAST(prd_start_dt AS date) as prd_start_dt ,
CAST(
    lead(prd_start_dt) over (PARTITION BY prd_key ORDER BY prd_start_dt) -1  AS date
) as prd_end_dt
from bronze.crm_prd_info 

;


TRUNCATE TABLE silver.crm_sales_details ;
INSERT into silver.crm_sales_details (
    sls_ord_num ,
    sls_prd_key,
    sls_cust_id,
    sls_order_dt, 
    sls_ship_dt ,
    sls_due_dt,
    sls_sales ,
    sls_quantity,
    sls_price ) 
SELECT 
    sls_ord_num ,
    sls_prd_key,
    sls_cust_id,
    CASE when sls_order_dt = 0 or LEN(sls_order_dt) != 8 then null else cast(cast(sls_order_dt as varchar) as date)
    end as sls_order_dt ,
    CASE when sls_ship_dt = 0 or LEN(sls_ship_dt) != 8 then null else cast(cast(sls_ship_dt as varchar) as date)
    end as sls_ship_dt,
    CASE when sls_due_dt = 0 or LEN(sls_due_dt) != 8 then null else cast(cast(sls_due_dt as varchar) as date) 
    end as sls_due_dt,
    CASE 
        when sls_sales is null or sls_sales <=0 or sls_sales != sls_quantity * ABS(sls_price) 
        then sls_quantity * ABS(sls_price)
        else sls_sales 
    END as sls_sales ,
    sls_quantity,
    CASE
        when sls_price is null or sls_price <= 0
        then sls_sales / nullif(sls_quantity , 0 ) 
        else sls_price 
    END AS sls_price 
from bronze.crm_sales_details ;


TRUNCATE TABLE silver.erp_cust_az12 ; 
INSERT into silver.erp_cust_az12(
    cid ,
    bdate,
    gen
)
SELECT
    -- Clean Customer ID: remove "NAS" prefix if exists
    CASE
        WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LEN(cid))
        ELSE cid
    END AS cid, 

    -- Birthdate: remove future dates (set to NULL)
    CASE
        WHEN TRY_CAST(bdate AS date) IS NULL THEN NULL                
        WHEN bdate > GETDATE() THEN NULL                             
        ELSE bdate
    END AS bdate,

    -- Gender: normalize to Male / Female / n/a and fix azura data studio issues 
  CASE 
        WHEN UPPER(TRIM(CHAR(13) + CHAR(10) + ' ' FROM gen)) IN ('M', 'MALE') 
            THEN 'Male'
        WHEN UPPER(TRIM(CHAR(13) + CHAR(10) + ' ' FROM gen)) IN ('F', 'FEMALE') 
            THEN 'Female'
        ELSE 'n/a'
    END AS gen

FROM bronze.erp_cust_az12;


TRUNCATE TABLE silver.erp_loc_a101;

INSERT into silver.erp_loc_a101(
    cid,
    cntry
)
SELECT 
    REPLACE(cid, '-', '') AS cid,
    CASE 
        WHEN UPPER(TRIM(CHAR(13) + CHAR(10) + CHAR(9) + ' ' FROM cntry)) = 'DE' 
            THEN 'Germany'
        WHEN UPPER(TRIM(CHAR(13) + CHAR(10) + CHAR(9) + ' ' FROM cntry)) IN ('US', 'USA') 
            THEN 'United States'
        WHEN TRIM(CHAR(13) + CHAR(10) + CHAR(9) + ' ' FROM cntry) = '' OR cntry IS NULL  
            THEN 'N/A'
        ELSE TRIM(CHAR(13) + CHAR(10) + CHAR(9) + ' ' FROM cntry)
    END AS cntry
FROM bronze.erp_loc_a101;

