/*
================================================================================
Stored Procedure: Load Bronze Layer (Source -> Bronze)
================================================================================

Script Purpose:

    This stored procedure loads data into the 'bronze' schema from external CSV files.
    It performs the following actions:

    - Truncates the bronze tables before loading data.
    - Uses the 'BULK INSERT' command to load data from CSV files to bronze tables.

Parameters:

    None.

    This stored procedure does not accept any parameters or return any values.

Usage Example:

    EXEC bronze.load_bronze;

================================================================================
*/

CREATE OR ALTER PROCEDURE bronze.load_bronze AS
BEGIN
DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME;
    BEGIN TRY
        SET @batch_start_time = GETDATE();


PRINT '=========================================='
PRINT 'Loading bronze layer'
PRINT '=========================================='

PRINT '------------------------------------------'
PRINT 'Loading CRM Tables'
PRINT '------------------------------------------'

SET @start_time = GETDATE();
TRUNCATE TABLE bronze.crm_cust_info;
BULK INSERT bronze.crm_cust_info
FROM 'C:\Users\princ\Downloads\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
WITH (FIRSTROW = 2, FIELDTERMINATOR = ',', TABLOCK
);
SET @end_time = GETDATE();
PRINT 'Time taken to load crm_cust_info: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS VARCHAR(10)) + ' seconds';


--Creating staging table to cater the bulk load data conversion errors
IF OBJECT_ID('bronze.crm_prd_info_staging','U') IS NOT NULL
    DROP TABLE bronze.crm_prd_info_staging;
CREATE TABLE bronze.crm_prd_info_staging (
    col1 INT,
    col2 VARCHAR(50),
    col3 VARCHAR(100),
    col4 INT,
    prd_start_dt VARCHAR(20),
    prd_end_dt VARCHAR(50)
);

-- Loading data into staging table
BULK INSERT bronze.crm_prd_info_staging
FROM 'C:\Users\princ\Downloads\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
WITH (FIRSTROW = 2, FIELDTERMINATOR = ',', TABLOCK
);

-- Migrating data from staging table to actual table with necessary conversions
SET @start_time = GETDATE();
TRUNCATE TABLE bronze.crm_prd_info;
INSERT INTO bronze.crm_prd_info (prd_id, prd_key, prd_nm, prd_cost, prd_start_dt, prd_end_dt)
SELECT
    col1,
    col2,
    col3,
    col4,
    TRY_CONVERT(DATE, prd_start_dt, 105) AS prd_start_dt,
    TRY_CONVERT(DATE, prd_end_dt, 105) AS prd_end_dt
FROM bronze.crm_prd_info_staging;
SET @end_time = GETDATE();
PRINT 'Time taken to load crm_prd_info: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS VARCHAR(10)) + ' seconds';

-- Dropping the staging table after successful migration
DROP TABLE bronze.crm_prd_info_staging;

SET @start_time = GETDATE();
TRUNCATE TABLE bronze.crm_sales_details;
BULK INSERT bronze.crm_sales_details
FROM 'C:\Users\princ\Downloads\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
WITH (FIRSTROW = 2, FIELDTERMINATOR = ',', TABLOCK
);
SET @end_time = GETDATE();
PRINT 'Time taken to load crm_sales_details: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS VARCHAR(10)) + ' seconds';


PRINT '------------------------------------------'
PRINT 'Loading ERP Tables'
PRINT '------------------------------------------'

SET @start_time = GETDATE();
TRUNCATE TABLE bronze.erp_cust_az12;
BULK INSERT bronze.erp_cust_az12
FROM 'C:\Users\princ\Downloads\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\cust_az12.csv'
WITH (FIRSTROW = 2, FIELDTERMINATOR = ',', TABLOCK
);
SET @end_time = GETDATE();
PRINT 'Time taken to load erp_cust_az12: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS VARCHAR(10)) + ' seconds';    

SET @start_time = GETDATE();
TRUNCATE TABLE bronze.erp_loc_a101;
BULK INSERT bronze.erp_loc_a101
FROM 'C:\Users\princ\Downloads\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\loc_a101.csv'
WITH (FIRSTROW = 2, FIELDTERMINATOR = ',', TABLOCK
);
SET @end_time = GETDATE();
PRINT 'Time taken to load erp_loc_a101: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS VARCHAR(10)) + ' seconds';

SET @start_time = GETDATE();
TRUNCATE TABLE bronze.erp_px_cat_g1v2;
BULK INSERT bronze.erp_px_cat_g1v2
FROM 'C:\Users\princ\Downloads\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\px_cat_g1v2.csv'
WITH (FIRSTROW = 2, FIELDTERMINATOR = ',', TABLOCK
);
SET @end_time = GETDATE();
PRINT 'Time taken to load erp_px_cat_g1v2: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS VARCHAR(10)) + ' seconds';

    SET @batch_end_time = GETDATE();
    PRINT '==============================================================='
    PRINT 'Bronze layer loaded successfully'
    PRINT 'Total Time taken to load bronze layer: ' + CAST(DATEDIFF(SECOND, @batch_start_time, @batch_end_time) AS VARCHAR(10)) + ' seconds';
    END TRY
    BEGIN CATCH
        PRINT '==============================================================='
        PRINT 'Error occurred while loading bronze layer'
        PRINT 'Error Message: ' + ERROR_MESSAGE()
        PRINT 'Error Message: ' + CAST(ERROR_NUMBER() AS VARCHAR(10))
        PRINT 'Error Message: ' + CAST(ERROR_STATE() AS VARCHAR(10))
    END CATCH


END
