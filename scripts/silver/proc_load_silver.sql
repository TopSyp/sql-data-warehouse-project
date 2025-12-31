CREATE OR ALTER PROCEDURE silver.load_silver AS
BEGIN
DECLARE @start_time DATETIME2, @end_time DATETIME2, @batch_start_time DATETIME2, @batch_end_time DATETIME2;
BEGIN TRY 
    SET @batch_start_time = GETDATE();

PRINT '==================================='
PRINT 'Loading Silver Layer'
PRINT '==================================='

PRINT '-----------------------------------'
PRINT 'Loading CRM tables'
PRINT '-----------------------------------'

-- Inserting cleaned and transformed data into silver.crm_cust_info
SET @start_time = GETDATE();
TRUNCATE TABLE silver.crm_cust_info
INSERT INTO silver.crm_cust_info(
    cst_id,
    cst_key,
    cst_firstname,
    cst_lastname,
    cst_marital_status,
    cst_creationdate,
    cst_gender
)
SELECT 
    cst_id,
    cst_key,
    TRIM(cst_firstname) AS cst_firstname,
    TRIM(cst_lastname) AS cst_lastname,
    CASE WHEN UPPER(cst_marital_status) = 'S' THEN 'Single'
         WHEN UPPER(cst_marital_status) = 'M' THEN 'Married'
         ELSE 'Unknown' END AS cst_marital_status,
    cst_creationdate,
    CASE WHEN UPPER(cst_gender) = 'F' THEN 'Female'
         WHEN UPPER(cst_gender) = 'M' THEN 'Male'
         ELSE 'Unknown' END AS cst_gender
FROM (SELECT *,
    ROW_NUMBER() OVER(PARTITION BY cst_id ORDER BY cst_creationdate DESC) AS flag_last
FROM bronze.crm_cust_info) t 
WHERE flag_last = 1 AND cst_id IS NOT NULL;
SET @end_time = GETDATE();
PRINT 'Time taken to load crm_cust_info: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS VARCHAR(10)) + ' seconds';

--Inserting transformed data into silver.crm_prd_info
SET @start_time = GETDATE();
TRUNCATE TABLE silver.crm_prd_info
INSERT INTO silver.crm_prd_info (prd_id, prd_key, cat_id, prd_nm, prd_cost, prd_line, prd_start_dt, prd_end_dt)
SELECT 
 prd_id,
 prd_key,
 REPLACE(SUBSTRING(prd_key,1,5),'-','_') AS cat_id,
 prd_nm,
 ISNULL(prd_cost, 0) AS prd_cost,
 CASE UPPER(TRIM(prd_line))
    WHEN 'M' THEN 'Mountain'
    WHEN 'R' THEN 'Road'
    WHEN 'S' THEN 'other sales'
    WHEN 'T' THEN 'touring'
    ELSE 'unknown'
 END AS prd_line,
 prd_start_dt,
 DATEADD(DAY, -1, LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt)) AS prd_end_dt
FROM bronze.crm_prd_info;
SET @end_time = GETDATE();
PRINT 'Time taken to load crm_prd_info: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS VARCHAR(10)) + ' seconds';

--Inserting cleaned data into silver table
SET @start_time = GETDATE();
TRUNCATE TABLE silver.crm_sales_details
INSERT INTO silver.crm_sales_details (sls_ord_num, sls_prd_key, sls_cust_id, sls_order_dt, sls_ship_dt, sls_due_dt, sls_sales, sls_quantity, sls_price)
SELECT
sls_ord_num,
sls_prd_key,
sls_cust_id,
 CASE WHEN sls_order_dt= 0 OR LEN(sls_order_dt) !=8 THEN NULL
    ELSE CAST(CAST(sls_order_dt AS VARCHAR(8)) AS DATE) END AS sls_order_dt,
 CASE WHEN sls_ship_dt= 0 OR LEN(sls_ship_dt) !=8 THEN NULL
    ELSE CAST(CAST(sls_ship_dt AS VARCHAR(8)) AS DATE) END AS sls_ship_dt,
 CASE WHEN sls_due_dt= 0 OR LEN(sls_due_dt) !=8 THEN NULL
    ELSE CAST(CAST(sls_due_dt AS VARCHAR(8)) AS DATE) END AS sls_due_dt,
CASE WHEN sls_sales IS NULL OR sls_sales <=0 OR sls_sales != ABS(sls_price) * sls_quantity
    THEN ABS(sls_price) * sls_quantity
    ELSE sls_sales END AS sls_sales,
sls_quantity,
CASE WHEN sls_price IS NULL OR sls_price <=0 
    THEN sls_sales / NULLIF(sls_quantity, 0)  
    ELSE sls_price END AS sls_price
FROM bronze.crm_sales_details
SET @end_time = GETDATE();
PRINT 'Time taken to load crm_sales_details: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS VARCHAR(10)) + ' seconds';

PRINT '-----------------------------------------'
PRINT 'Loading ERP tables'
PRINT '-----------------------------------------'


-- Final Insert Statement
SET @start_time = GETDATE();
TRUNCATE TABLE silver.erp_cust_az12
INSERT INTO silver.erp_cust_az12 (cid, bdate, gen)
SELECT  
    CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LEN(cid)) 
    ELSE cid END AS cid,
    CASE WHEN bdate > GETDATE() THEN NULL
    ELSE bdate END AS bdate,
    CASE 
        WHEN UPPER(TRIM(gen)) IN ('M', 'MALE') THEN 'Male'
        WHEN UPPER(TRIM(gen)) IN ('F', 'FEMALE') THEN 'Female'
        ELSE 'unknown'
    END AS gen
FROM bronze.erp_cust_az12
SET @end_time = GETDATE();
PRINT 'Time taken to load erp_cust_az12: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS VARCHAR(10)) + ' seconds';


-- Final Insert Statement
SET @start_time = GETDATE();
TRUNCATE TABLE silver.erp_px_cat_g1v2
INSERT INTO silver.erp_px_cat_g1v2 (id,cat,subcat,maintenance)
SELECT id,cat,subcat,maintenance
FROM bronze.erp_px_cat_g1v2;
SET @end_time = GETDATE();
PRINT 'Time taken to load erp_px_cat_g1v2: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS VARCHAR(10)) + ' seconds';

--- Final Insert Statement
SET @start_time = GETDATE();
TRUNCATE TABLE silver.erp_px_cat_g1v2
INSERT INTO silver.erp_loc_a101 (cid, cntry)
SELECT 
    REPLACE (cid, '-','') AS cid,
    CASE WHEN TRIM(cntry) IN ('US', 'USA') THEN 'United States'
     WHEN TRIM(cntry) = 'DE' THEN 'Germany'
     WHEN TRIM(cntry) IS NULL OR TRIM(cntry) = '' THEN 'Unknown'
     ELSE TRIM(cntry)
END AS cntry
FROM bronze.erp_loc_a101;
SET @end_time = GETDATE();
PRINT 'Time taken to load erp_loc_a101: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS VARCHAR(10)) + ' seconds';

SET @batch_end_time = GETDATE();
PRINT '======================================================'
PRINT 'Silver Layer Load Completed Successfully'
PRINT 'Total time taken to load Silver Layer: ' + CAST(DATEDIFF(SECOND, @batch_start_time, @batch_end_time) AS VARCHAR(10)) + ' seconds';
PRINT '======================================================'

END TRY 
BEGIN CATCH
        PRINT '==============================================================='
        PRINT 'Error occurred while loading bronze layer'
        PRINT 'Error Message: ' + ERROR_MESSAGE()
        PRINT 'Error Message: ' + CAST(ERROR_NUMBER() AS VARCHAR(10))
        PRINT 'Error Message: ' + CAST(ERROR_STATE() AS VARCHAR(10))
    END CATCH

END;
