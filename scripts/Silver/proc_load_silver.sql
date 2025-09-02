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

