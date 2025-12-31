SELECT
sls_ord_num,
sls_prd_key,
sls_cust_id,
sls_order_dt,
sls_ship_dt,
sls_due_dt,
sls_sales,
sls_quantity,
sls_price
FROM bronze.crm_sales_details

--Check for invalid dates
SELECT
    sls_order_dt
FROM bronze.crm_sales_details
WHERE sls_order_dt <= 0 OR LEN(sls_order_dt) != 8;

--Convert integer dates to proper date format with handling for invalid dates
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
sls_sales,
sls_quantity,
sls_price
FROM bronze.crm_sales_details

--Check for inconsistent sales, quantity, price data
SELECT sls_sales,
 sls_quantity, 
 sls_price 
FROM bronze.crm_sales_details
WHERE sls_sales != sls_price * sls_quantity
OR sls_sales IS NULL OR sls_quantity IS NULL OR sls_price IS NULL
OR sls_sales <= 0 OR sls_quantity <= 0 OR sls_price <= 0
ORDER BY sls_sales, sls_quantity, sls_price;

--Making changes in silver table for cleaned data
IF OBJECT_ID('silver.crm_sales_details','U') IS NOT NULL
    DROP TABLE silver.crm_sales_details;
GO
CREATE TABLE silver.crm_sales_details(
    sls_ord_num VARCHAR(50),
    sls_prd_key VARCHAR(50),
    sls_cust_id INT,
    sls_order_dt DATE,
    sls_ship_dt DATE,
    sls_due_dt DATE,
    sls_sales INT,
    sls_quantity INT,
    sls_price INT,
    dwh_create_date DATETIME2 DEFAULT GETDATE()
);
GO

--Final corrected data selection
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

--Inserting cleaned data into silver table
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
