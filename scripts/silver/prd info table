USE DataWarehouseDB

SELECT *
FROM bronze.crm_prd_info;

--Check for nulls or duplicates in primary key column prd_id
--Expectation: No nulls and no duplicates
SELECT 
prd_id,
COUNT(*) AS cnt_duplicates
FROM bronze.crm_prd_info
GROUP BY prd_id
HAVING COUNT(*) > 1;


-- Data transformation: Create cat_id by extracting first 5 characters from prd_key and replacing '-' with '_'
SELECT 
 prd_id,
 prd_key,
 REPLACE(SUBSTRING(prd_key,1,5),'-','_') AS cat_id,
 prd_nm,
 prd_cost,
 prd_line,
 prd_start_dt,
 prd_end_dt
FROM bronze.crm_prd_info;

-- Data validation: Ensure all cat_id values exist in reference table bronze.erp_px_cat_g1v2
SELECT 
 prd_id,
 prd_key,
 REPLACE(SUBSTRING(prd_key,1,5),'-','_') AS cat_id,
 SUBSTRING(prd_key,7, LEN(prd_key)) AS prd_key,
 prd_nm,
 prd_cost,
 prd_line,
 prd_start_dt,
 prd_end_dt
FROM bronze.crm_prd_info
WHERE  REPLACE(SUBSTRING(prd_key,1,5),'-','_')  NOT IN 
    (SELECT DISTINCT id FROM bronze.erp_px_cat_g1v2);


--- Data validation: Ensure all prd_key values exist in reference table bronze.crm_sales_details
SELECT 
 prd_id,
 prd_key,
 REPLACE(SUBSTRING(prd_key,1,5),'-','_') AS cat_id,
 SUBSTRING(prd_key,7, LEN(prd_key)) AS prd_key,
 prd_nm,
 prd_cost,
 prd_line,
 prd_start_dt,
 prd_end_dt
FROM bronze.crm_prd_info
WHERE SUBSTRING(prd_key,7, LEN(prd_key))  IN 
    (SELECT DISTINCT sls_prd_key FROM bronze.crm_sales_details);

--- Check for leading or trailing spaces in prd_nm
SELECT prd_nm
FROM bronze.crm_prd_info
WHERE prd_nm != TRIM(prd_nm);

-- Check for negative or null values in prd_cost
SELECT prd_cost
FROM bronze.crm_prd_info
WHERE prd_cost < 0 OR prd_cost IS NULL


--Replacing null prd_cost with 0
SELECT 
 prd_id,
 prd_key,
 REPLACE(SUBSTRING(prd_key,1,5),'-','_') AS cat_id,
 SUBSTRING(prd_key,7, LEN(prd_key)) AS prd_key,
 prd_nm,
 ISNULL(prd_cost, 0) AS prd_cost,
 prd_line,
 prd_start_dt,
 prd_end_dt
FROM bronze.crm_prd_info

--Data standardization: Converting prd_line to fullform
SELECT 
 prd_id,
 prd_key,
 REPLACE(SUBSTRING(prd_key,1,5),'-','_') AS cat_id,
 SUBSTRING(prd_key,7, LEN(prd_key)) AS prd_key,
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
 prd_end_dt
FROM bronze.crm_prd_info;

--check for invalid date orders
SELECT *
FROM bronze.crm_prd_info
WHERE prd_end_dt < prd_start_dt;



--Deriving prd_end_dt based on prd_start_dt of next record for same prd_key
SELECT 
 prd_id,
 prd_key,
 REPLACE(SUBSTRING(prd_key,1,5),'-','_') AS cat_id,
 SUBSTRING(prd_key,7, LEN(prd_key)) AS prd_key,
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

--Modifying silver.crm_prd_info table to include cat_id column
IF OBJECT_ID('silver.crm_prd_info','U') IS NOT NULL
    DROP TABLE silver.crm_prd_info;
GO
CREATE TABLE silver.crm_prd_info(
    prd_id INT,
    prd_key VARCHAR(50),
    cat_id VARCHAR(50),
    prd_nm VARCHAR(100),
    prd_cost INT,
    prd_line VARCHAR(20),
    prd_start_dt DATE,
    prd_end_dt DATE,
    dwh_create_date DATETIME2 DEFAULT GETDATE()
);

--Inserting transformed data into silver.crm_prd_info
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
