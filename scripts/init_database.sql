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
