/*
   this file to create stored procedure 
   run it first to create it 
   then run this query:
   exec silver.load_silver
   =======================================
   this code includes error handling using try catch 
   and calculates duration for each process and the whole patch
*/

CREATE OR ALTER PROCEDURE silver.load_silver AS
BEGIN
    DECLARE @start_time DATETIME, @end_time DATETIME, @start_whole_time DATETIME, @end_whole_time DATETIME;

    BEGIN TRY
        SET @start_whole_time = GETDATE();

        PRINT '****************************************************'
        PRINT '  START LOADING SILVER LAYER'
        PRINT '****************************************************'

        --------------------------------------------------------
        PRINT '=============== CRM TRANSFORMATIONS ================='
        --------------------------------------------------------

        -- crm_cust_info
        SET @start_time = GETDATE();
        PRINT '>> TRUNCATING TABLE: silver.crm_cust_info'
        TRUNCATE TABLE silver.crm_cust_info;

        PRINT '>> Inserting Data into: silver.crm_cust_info'
        INSERT INTO silver.crm_cust_info (
            cst_id,
            cst_key,
            cst_firstname,
            cst_lastname,
            cst_marital_status,
            cst_gndr,
            cst_create_date
        )
        SELECT
            cst_id,
            cst_key,
            TRIM(cst_firstname) AS cst_firstname,
            TRIM(cst_lastname) AS cst_lastname,
            CASE 
                WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
                WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
                ELSE 'N/A'
            END AS cst_marital_status,
            CASE 
                WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
                WHEN UPPER(TRIM(cst_gndr)) = 'S' THEN 'Female'
                ELSE 'N/A'
            END AS cst_gndr,
            cst_create_date
        FROM (
            SELECT *,
                   ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag_last
            FROM bronze.crm_cust_info
            WHERE cst_id IS NOT NULL
        ) t
        WHERE flag_last = 1;
        SET @end_time = GETDATE();
        PRINT 'LOAD DURATION (crm_cust_info): ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';

        -- crm_prd_info
        SET @start_time = GETDATE();
        PRINT '>> TRUNCATING TABLE: silver.crm_prd_info'
        TRUNCATE TABLE silver.crm_prd_info;

        PRINT '>> Inserting Data into: silver.crm_prd_info'
        INSERT INTO silver.crm_prd_info (
            prd_id,
            cat_id,
            prd_key,
            prd_nm,
            prd_cost,
            prd_line,
            prd_start_dt,
            prd_end_dt
        )
        SELECT
            prd_id,
            REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') AS cat_id,
            SUBSTRING(prd_key, 7, LEN(prd_key)) AS prd_key,
            prd_nm,
            ISNULL(prd_cost, 0) AS prd_cost,
            CASE
                WHEN UPPER(TRIM(prd_line)) = 'R' THEN 'Road'
                WHEN UPPER(TRIM(prd_line)) = 'M' THEN 'Mountain'
                WHEN UPPER(TRIM(prd_line)) = 'S' THEN 'Other Sales'
                WHEN UPPER(TRIM(prd_line)) = 'T' THEN 'Touring'
                ELSE 'N/A'
            END AS prd_line,
            CAST(prd_start_dt AS DATE) AS prd_start_dt,
            CAST(LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt) - 1 AS DATE) AS prd_end_dt
        FROM bronze.crm_prd_info;
        SET @end_time = GETDATE();
        PRINT 'LOAD DURATION (crm_prd_info): ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';

        -- crm_sales_details
        SET @start_time = GETDATE();
        PRINT '>> TRUNCATING TABLE: silver.crm_sales_details'
        TRUNCATE TABLE silver.crm_sales_details;

        PRINT '>> Inserting Data into: silver.crm_sales_details'
        INSERT INTO silver.crm_sales_details (
            sls_ord_num,
            sls_prd_key,
            sls_cust_id,
            sls_order_dt,
            sls_ship_dt,
            sls_due_dt,
            sls_sales,
            sls_quantity,
            sls_price
        )
        SELECT
            sls_ord_num,
            sls_prd_key,
            sls_cust_id,
            CASE WHEN sls_order_dt = 0 OR LEN(sls_order_dt) != 8 THEN NULL ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE) END AS sls_order_dt,
            CASE WHEN sls_ship_dt = 0 OR LEN(sls_ship_dt) != 8 THEN NULL ELSE CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE) END AS sls_ship_dt,
            CASE WHEN sls_due_dt = 0 OR LEN(sls_due_dt) != 8 THEN NULL ELSE CAST(CAST(sls_due_dt AS VARCHAR) AS DATE) END AS sls_due_dt,
            CASE 
                WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales != sls_quantity * ABS(sls_price) 
                THEN sls_quantity * ABS(sls_price)
                ELSE sls_sales 
            END AS sls_sales,
            sls_quantity,
            CASE
                WHEN sls_price IS NULL OR sls_price <= 0
                THEN sls_sales / NULLIF(sls_quantity, 0)
                ELSE sls_price
            END AS sls_price
        FROM bronze.crm_sales_details;
        SET @end_time = GETDATE();
        PRINT 'LOAD DURATION (crm_sales_details): ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';

        --------------------------------------------------------
        PRINT '=============== ERP TRANSFORMATIONS ================='
        --------------------------------------------------------

        -- erp_cust_az12
        SET @start_time = GETDATE();
        PRINT '>> TRUNCATING TABLE: silver.erp_cust_az12'
        TRUNCATE TABLE silver.erp_cust_az12;

        PRINT '>> Inserting Data into: silver.erp_cust_az12'
        INSERT INTO silver.erp_cust_az12 (
            cid,
            bdate,
            gen
        )
        SELECT
            CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LEN(cid)) ELSE cid END AS cid,
            CASE
                WHEN TRY_CAST(bdate AS DATE) IS NULL THEN NULL
                WHEN bdate > GETDATE() THEN NULL
                ELSE bdate
            END AS bdate,
            CASE
                WHEN UPPER(TRIM(CHAR(13) + CHAR(10) + ' ' FROM gen)) IN ('M', 'MALE') THEN 'Male'
                WHEN UPPER(TRIM(CHAR(13) + CHAR(10) + ' ' FROM gen)) IN ('F', 'FEMALE') THEN 'Female'
                ELSE 'n/a'
            END AS gen
        FROM bronze.erp_cust_az12;
        SET @end_time = GETDATE();
        PRINT 'LOAD DURATION (erp_cust_az12): ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';

        -- erp_loc_a101
        SET @start_time = GETDATE();
        PRINT '>> TRUNCATING TABLE: silver.erp_loc_a101'
        TRUNCATE TABLE silver.erp_loc_a101;

        PRINT '>> Inserting Data into: silver.erp_loc_a101'
        INSERT INTO silver.erp_loc_a101 (
            cid,
            cntry
        )
        SELECT
            REPLACE(cid, '-', '') AS cid,
            CASE 
                WHEN UPPER(TRIM(CHAR(13) + CHAR(10) + CHAR(9) + ' ' FROM cntry)) = 'DE' THEN 'Germany'
                WHEN UPPER(TRIM(CHAR(13) + CHAR(10) + CHAR(9) + ' ' FROM cntry)) IN ('US', 'USA') THEN 'United States'
                WHEN TRIM(CHAR(13) + CHAR(10) + CHAR(9) + ' ' FROM cntry) = '' OR cntry IS NULL THEN 'N/A'
                ELSE TRIM(CHAR(13) + CHAR(10) + CHAR(9) + ' ' FROM cntry)
            END AS cntry
        FROM bronze.erp_loc_a101;
        SET @end_time = GETDATE();
        PRINT 'LOAD DURATION (erp_loc_a101): ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';

        -- erp_px_cat_g1v2
        SET @start_time = GETDATE();
        PRINT '>> TRUNCATING TABLE: silver.erp_px_cat_g1v2'
        TRUNCATE TABLE silver.erp_px_cat_g1v2;

        PRINT '>> Inserting Data into: silver.erp_px_cat_g1v2'
        INSERT INTO silver.erp_px_cat_g1v2 (
            id,
            cat,
            subcat,
            maintenance
        )
        SELECT
            id,
            cat,
            subcat,
            maintenance
        FROM bronze.erp_px_cat_g1v2;
        SET @end_time = GETDATE();
        PRINT 'LOAD DURATION (erp_px_cat_g1v2): ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';

        --------------------------------------------------------
        PRINT '****************************************************'
        PRINT ' SILVER LOAD COMPLETED SUCCESSFULLY'
        PRINT '****************************************************'
        --------------------------------------------------------

        SET @end_whole_time = GETDATE();
        PRINT 'TOTAL DATA LOAD TIME: ' + CAST(DATEDIFF(second, @start_whole_time, @end_whole_time) AS NVARCHAR) + ' SECONDS';

    END TRY
    BEGIN CATCH
        PRINT '*************************************'
        PRINT ' Error message: ' + ERROR_MESSAGE();
        PRINT ' Error at line: ' + CAST(ERROR_LINE() AS NVARCHAR);
        PRINT '*************************************'
    END CATCH
END;
