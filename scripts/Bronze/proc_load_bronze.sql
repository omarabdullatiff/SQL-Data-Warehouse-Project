/* 
   this file to create stores procedure 
   run it first to  create it 
   the run this query 
   exec bronze.load_bronze
   =======================================
   this code include error handling using try catch 
   and calculate duration FOR each process and the patch 
*/


CREATE OR ALTER PROCEDURE bronze.load_bronze AS
BEGIN
    DECLARE @start_time DATETIME , @end_time DATETIME ,@start_whole_time DATETIME , @end_whole_time DATETIME;
    BEGIN TRY
        set @start_whole_time = GETDATE()
        PRINT '*****************************'
        PRINT 'START LOADING BRONZE LAYER'
        PRINT '*****************************'

        PRINT '*****************************'
        PRINT 'LOADING CRM TABLES'
        PRINT '*****************************'
        set @start_time = GETDATE()
        PRINT '>> TRUNCATING TABLE:bronze.crm_cust_info'
        TRUNCATE TABLE bronze.crm_cust_info ;

        PRINT '>> Inserting Data into:bronze.crm_cust_info'

        BULK insert bronze.crm_cust_info 
        FROM '/var/opt/mssql/data/cust_info.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            tablock 
        
        );
        SET @end_time = GETDATE();
        PRINT 'LOAD DURATION:' +cast(DATEDIFF(second, @start_time ,@end_time) AS NVARCHAR) + 'seconds';
        set @start_time = GETDATE();
        PRINT '>> TRUNCATING TABLE:bronze.crm_prd_info'
        TRUNCATE TABLE bronze.crm_prd_info ;
        PRINT '>> Inserting Data into:bronze.crm_prd_info '

        BULK insert bronze.crm_prd_info 
        from '/var/opt/mssql/data/datasets/source_crm/prd_info.csv'
        with(
            firstrow = 2,
            FIELDTERMINATOR = ',',
            tablock 
        )
        ;
        SET @end_time = GETDATE();
        PRINT 'LOAD DURATION:' +cast(DATEDIFF(second, @start_time ,@end_time) AS NVARCHAR) + 'seconds';
 
 
        set @start_time = GETDATE();

        PRINT '>> TRUNCATING TABLE:bronze.crm_sales_details'
        TRUNCATE TABLE bronze.crm_sales_details ; 

        PRINT '>> Inserting Data into:bronze.crm_sales_details '

        bulk insert bronze.crm_sales_details 
        from '/var/opt/mssql/data/datasets/source_crm/sales_details.csv'
        with(
            firstrow = 2 ,
            fieldterminator = ','
            ,tablock
        );
        SET @end_time = GETDATE();
        PRINT 'LOAD DURATION:' +cast(DATEDIFF(second, @start_time ,@end_time) AS NVARCHAR) + 'seconds';



        PRINT '*****************************'
        PRINT 'LOADING ERP TABLES'
        PRINT '*****************************'
        set @start_time = GETDATE();

        PRINT '>> TRUNCATING TABLE:bronze.erp_cust_az12'
        TRUNCATE TABLE bronze.erp_cust_az12 ;

        PRINT '>> Inserting Data into:bronze.erp_cust_az12 '

        bulk insert bronze.erp_cust_az12 
        from '/var/opt/mssql/data/datasets/source_erp/CUST_AZ12.csv'
        with(
            firstrow = 2 ,
            fieldterminator = ',',
            tablock  
        );

        SET @end_time = GETDATE();
        PRINT 'LOAD DURATION:' +cast(DATEDIFF(second, @start_time ,@end_time) AS NVARCHAR) + 'seconds';

        set @start_time = GETDATE();

        PRINT '>> TRUNCATING TABLE:bronze.erp_loc_a101'
        TRUNCATE TABLE bronze.erp_loc_a101 ;

        PRINT '>> Inserting Data into:bronze.erp_loc_a101 '


        bulk insert bronze.erp_loc_a101
        from '/var/opt/mssql/data/datasets/source_erp/LOC_A101.csv'
        with(
            firstrow = 2 ,
            fieldterminator = ',',
            tablock  
        );
        SET @end_time = GETDATE();
        PRINT 'LOAD DURATION:' +cast(DATEDIFF(second, @start_time ,@end_time) AS NVARCHAR) + 'seconds';


        set @start_time = GETDATE();

        PRINT '>> TRUNCATING TABLE:bronze.erp_px_cat_g1v2'

        TRUNCATE TABLE bronze.erp_px_cat_g1v2 ;

        PRINT '>> Inserting Data into:bronze.erp_px_cat_g1v2 '

        bulk insert bronze.erp_px_cat_g1v2
        from '/var/opt/mssql/data/datasets/source_erp/PX_CAT_G1V2.csv'
        with(
            firstrow = 2 ,
            fieldterminator = ',',
            tablock  
        );
        SET @end_time = GETDATE();
        PRINT 'LOAD DURATION:' +cast(DATEDIFF(second, @start_time ,@end_time) AS NVARCHAR) + 'seconds';

    PRINT 'Data is loaded successfully'
    set @end_whole_time = GETDATE()
    PRINT 'DATA LOADED IN: ' + CAST(DATEDIFF(second ,@start_whole_time ,@end_whole_time) AS NVARCHAR) +  ' SECONDS'
    END TRY
    BEGIN CATCH
    PRINT '*************************************'
    PRINT 'Error message: ' + ERROR_MESSAGE();
    PRINT 'Error at line:'  + CAST(ERROR_LINE() AS NVARCHAR);
    PRINT '*************************************'
    
    END CATCH
END  
