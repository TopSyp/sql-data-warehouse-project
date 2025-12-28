/*

=======================================================
Create database and schemas
=======================================================
Script Purpose :
  This script creates a new database named DataWarehouse' after checking if it already exists.
If the database exists it is dropped and recreated. Additionally,the script set ups three schema in the database : 'bronze',' silver' and 'gold'.
*/

USE master;
GO
  
--Drop and recreate the 'DataWaarehouseDB' Database
IF EXISTS (SELECT 1 FROM sys.databases WHERE name = 'DataWarehouseDB')
  BEGIN
    ALTER DATABASE DataWarehouseDB SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
    DROP DATABASE DataWarehouseDB;
  END;
GO

--Create 'DataWarrehouseDB' Database
CREATE DATABASE DataWarehouseDB;
GO 
  
USE DataWarehouseDB;
GO

-- Create Schemas

CREATE SCHEMA bronze; 
GO
  
CREATE SCHEMA silver;
GO
  
CREATE SCHEMA gold;
GO


--Creating tables in bronze schema

IF OBJECT_ID('bronze.crm_cust_info','U') IS NOT NULL
    DROP TABLE bronze.crm_cust_info;
GO
CREATE TABLE bronze.crm_cust_info(
    cst_id INT,
    cst_key VARCHAR(50),
    cst_firstname VARCHAR(50),
    cst_lastname VARCHAR(50),
    cst_marital_status VARCHAR(15),
    cst_gender VARCHAR(10),
    cst_creationdate DATE
);
GO

IF OBJECT_ID('bronze.crm_prd_info','U') IS NOT NULL
    DROP TABLE bronze.crm_prd_info;
GO
CREATE TABLE bronze.crm_prd_info(
    prd_id INT,
    prd_key VARCHAR(50),
    prd_nm VARCHAR(100),
    prd_cost FLOAT,
    prd_start_dt DATE,
    prd_end_dt DATE
);
GO

IF OBJECT_ID('bronze.crm_sales_details','U') IS NOT NULL
    DROP TABLE bronze.crm_sales_details;
GO
CREATE TABLE bronze.crm_sales_details(
    sls_ord_num VARCHAR(50),
    sls_prd_key VARCHAR(50),
    sls_cust_id INT,
    sls_order_dt DATE,
    sls_ship_dt DATE,
    sls_due_dt DATE,
    sls_sales INT,
    sls_quantity INT,
    sls_price FLOAT
);
GO

IF OBJECT_ID('bronze.erp_cust_az12','U') IS NOT NULL
    DROP TABLE bronze.erp_cust_az12;
GO
CREATE TABLE bronze.erp_cust_az12(
    cid VARCHAR(50),
    bdate DATE,
    gen VARCHAR(10)
);
GO

IF OBJECT_ID('bronze.erp_loc_a101','U') IS NOT NULL
    DROP TABLE bronze.erp_loc_a101;
GO
CREATE TABLE bronze.erp_loc_a101(
    cid VARCHAR(50),
    cntry VARCHAR(50)   
);
GO

IF OBJECT_ID('bronze.erp_px_cat_g1v2','U') IS NOT NULL
    DROP TABLE bronze.erp_px_cat_g1v2;
GO
CREATE TABLE bronze.erp_px_cat_g1v2(
    id VARCHAR(50),
    cat VARCHAR(50),
    subcat VARCHAR(50),
    maintenance VARCHAR(10)
);
GO

-- Loading data into tables from csv files.

TRUNCATE TABLE bronze.crm_cust_info;
BULK INSERT bronze.crm_cust_info
FROM 'C:\Users\princ\Downloads\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
WITH (FIRSTROW = 2, FIELDTERMINATOR = ',', TABLOCK
);
