
-- Clean customer IDs by removing dashes
SELECT   *,
REPLACE (cid, '-','') AS cid_cleaned
FROM bronze.erp_loc_a101

-- Standardize country codes
SELECT DISTINCT cntry, 
CASE WHEN TRIM(cntry) IN ('US', 'USA') THEN 'United States'
     WHEN TRIM(cntry) = 'DE' THEN 'Germany'
     WHEN TRIM(cntry) IS NULL OR TRIM(cntry) = '' THEN 'Unknown'
     ELSE TRIM(cntry)
END AS cntry_cleaned
FROM bronze.erp_loc_a101;


--- Final Insert Statement
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
