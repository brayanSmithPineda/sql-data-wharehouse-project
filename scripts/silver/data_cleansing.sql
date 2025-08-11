/*
===============================================================================
PostgreSQL Script: Data Cleansing
===============================================================================
Script Purpose:
    This script cleans the data in the silver schema tables.

Note:
    This script is used to clean the data in the silver schema tables.
    For data cleansing you need to check each column and make sure the data is valid.

Data Validation and Transformation:
    1. Check for duplicates and null values in the primary keys
    2. Check unwanted spaces in string columns
    3. Data standarization (Normalization) & consistency checks (handle nulls, Category names, etc.)
    4. Check data types and convert to the correct data type (e.g. date, time, etc.)
    5. Check the primary key, should we split the primary key into multiple columns?
        if we do it then how we can use those columns to join the tables?
    6. The dates, initial and end dates, make sense?
        Two conditions should be met:
            1. the initial date should be before the end date
            2. sometimes does not make sense that the periods overlap
    
===============================================================================
*/

--1. Duplicates and null values in the primary keys
SELECT cst_id, COUNT(*)
FROM bronze.crm_cust_info
GROUP BY cst_id
HAVING COUNT(*) > 1 OR cst_id IS NULL; -- this will show us the primary keys that are duplicated or null

SELECT *
FROM bronze.crm_cust_info
LIMIT 10;
--2. Check unwanted spaces in string columns
SELECT cst_firstname, cst_lastname, cst_marital_status, cst_gndr
FROM bronze.crm_cust_info
WHERE cst_firstname LIKE '% %'
    OR cst_lastname LIKE '% %'
    OR cst_marital_status LIKE '% %'
    OR cst_gndr LIKE '% %';

--3. Data standarization (Normalization) & consistency checks (handle nulls, Category names, etc.)
SELECT cst_marital_status, cst_gndr
FROM bronze.crm_cust_info

SELECT *
FROM bronze.crm_cust_info

--4. Data Type
SELECT
    column_name,
    data_type
FROM information_schema.columns
WHERE table_schema = 'bronze'
    AND table_name = 'crm_cust_info'
    AND column_name = 'cst_create_date';


/*
===============================================================================
    Cleaning and inserting data into the silver.crm_cust_info table
===============================================================================
*/

CREATE OR REPLACE PROCEDURE  silver.silver_load_silver()
LANGUAGE plpgsql
AS $$
DECLARE
    -- declare variables to store the start and end times of the data cleansing process
    start_time TIMESTAMP;
    end_time TIMESTAMP;
    section_start TIMESTAMP;
    section_duration INTERVAL;
    total_duration INTERVAL;
BEGIN
    -- clock_timestamp() is used to get the current timestamp
    -- := is used to assign the value to the variable, if we use = it will just print the value
    start_time := clock_timestamp();
    -- RAISE NOTICE is used to print the message to the console
    RAISE NOTICE 'Starting data cleansing process at: %', start_time;
    
    -- Section 1: Cleaning and inserting data into silver.crm_cust_info table
    -- clock_timestamp() is used to get the current timestamp
    -- := is used to assign the value to the variable, if we use = it will just print the value
    section_start := clock_timestamp();
    -- RAISE NOTICE is used to print the message to the console
    -- Section 1: Cleaning and inserting data into silver.crm_cust_info table
    section_start := clock_timestamp();
    RAISE NOTICE 'Starting CRM Customer Info processing...';
    
    -- Section operations are automatically in a transaction within the stored procedure
    -- truncate/remove the data before inserting new data, CASCADE is used to remove the foreign key constraints
    -- that way we do not repeat the data
    
    TRUNCATE TABLE silver.crm_cust_info CASCADE;    
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
        CASE WHEN cst_marital_status = 'M' THEN 'Married'
            WHEN cst_marital_status = 'S' THEN 'Single'
            ELSE 'N/A'
        END AS cst_marital_status,
        CASE WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
            WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
            ELSE 'N/A'
        END AS cst_gndr,
        cst_create_date 
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date) AS flag_duplicate
        FROM bronze.crm_cust_info
    ) AS subquery
    WHERE flag_duplicate = 1;
    
    section_duration := clock_timestamp() - section_start;
    RAISE NOTICE 'CRM Customer Info processing completed in: %', section_duration;

    /*
    ===============================================================================
        Cleaning and inserting data into the silver.crm_prd_info table
    ===============================================================================
    */
    -- Section 2: Cleaning and inserting data into silver.crm_prd_info table
    section_start := clock_timestamp();
    RAISE NOTICE 'Starting CRM Product Info processing...';
    
    -- truncate/remove the data before inserting new data, CASCADE is used to remove the foreign key constraints
    -- that way we do not repeat the data
    TRUNCATE TABLE silver.crm_prd_info CASCADE;
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
        TRIM(SUBSTRING(prd_key, 7, LENGTH(prd_key))) AS prd_key,
        prd_nm,
        COALESCE(prd_cost, 0) AS prd_cost,
        CASE TRIM(UPPER(prd_line))
            WHEN 'M' THEN 'Mountain'
            WHEN 'R' THEN 'Road'
            WHEN 'T' THEN 'Touring'
            WHEN 'S' THEN 'Other sales'
            ELSE 'N/A'
        END AS prd_line,
    prd_start_dt::DATE AS prd_start_dt,
    LEAD(prd_start_dt::DATE) OVER (PARTITION BY prd_key ORDER BY prd_start_dt) -1 AS prd_end_dt
    FROM bronze.crm_prd_info;
    
    section_duration := clock_timestamp() - section_start;
    RAISE NOTICE 'CRM Product Info processing completed in: %', section_duration;

    /*
    ===============================================================================
        Cleaning and inserting data into the silver.crm_sales_details table
    ===============================================================================
    */
    -- Section 3: Cleaning and inserting data into silver.crm_sales_details table
    section_start := clock_timestamp();
    RAISE NOTICE 'Starting CRM Sales Details processing...';

    -- truncate/remove the data before inserting new data, CASCADE is used to remove the foreign key constraints
    -- that way we do not repeat the data
    TRUNCATE TABLE silver.crm_sales_details CASCADE;
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
        CASE 
            WHEN sls_order_dt IS NULL OR LENGTH(sls_order_dt::TEXT) != 8 THEN NULL
            ELSE TO_DATE(sls_order_dt::TEXT, 'YYYYMMDD')
        END AS sls_order_dt,
        CASE 
            WHEN sls_ship_dt IS NULL OR LENGTH(sls_ship_dt::TEXT) != 8 THEN NULL
            ELSE TO_DATE(sls_ship_dt::TEXT, 'YYYYMMDD')
        END AS sls_ship_dt,
        CASE 
            WHEN sls_due_dt IS NULL OR LENGTH(sls_due_dt::TEXT) != 8 THEN NULL
            ELSE TO_DATE(sls_due_dt::TEXT, 'YYYYMMDD')
        END AS sls_due_dt,
        CASE 
            WHEN sls_sales != ABS(sls_price)*ABS(sls_quantity) OR sls_sales IS NULL OR sls_sales <= 0 THEN ABS(sls_price)*ABS(sls_quantity)
            ELSE sls_sales
        END AS sls_sales,
        sls_quantity,
        CASE 
            WHEN sls_price IS NULL OR sls_price <= 0 THEN ABS(sls_sales)/NULLIF(ABS(sls_quantity), 0)
            ELSE ABS(sls_price)
        END AS sls_price
    FROM bronze.crm_sales_details;
    
    section_duration := clock_timestamp() - section_start;
    RAISE NOTICE 'CRM Sales Details processing completed in: %', section_duration;

    /*
    ===============================================================================
        Cleaning and inserting data into the silver.erp_cust_az12 table
    ===============================================================================
    */
    -- Section 4: Cleaning and inserting data into silver.erp_cust_az12 table
    section_start := clock_timestamp();
    RAISE NOTICE 'Starting ERP Customer AZ12 processing...';
    
    -- truncate/remove the data before inserting new data, CASCADE is used to remove the foreign key constraints
    -- that way we do not repeat the data

    TRUNCATE TABLE silver.erp_cust_az12 CASCADE;
    INSERT INTO silver.erp_cust_az12 (
        cid,
        bdate,
        gen
    )
    SELECT
        CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LENGTH(cid)) ELSE cid END AS cid,
        CASE WHEN bdate > NOW() THEN NULL ELSE bdate END AS bdate,
        CASE 
            WHEN UPPER(TRIM(gen)) IN ('M', 'MALE') THEN 'Male'
            WHEN UPPER(TRIM(gen)) IN ('F', 'FEMALE') THEN 'Female'
            ELSE 'N/A'
        END AS gen
    FROM bronze.erp_cust_az12;
    
    section_duration := clock_timestamp() - section_start;
    RAISE NOTICE 'ERP Customer AZ12 processing completed in: %', section_duration;

    /*
    ===============================================================================
        Cleaning and inserting data into the silver.erp_loc_a101 table
    ===============================================================================
    */
    -- Section 5: Cleaning and inserting data into silver.erp_loc_a101 table
    section_start := clock_timestamp();
    RAISE NOTICE 'Starting ERP Location A101 processing...';
    
    -- truncate/remove the data before inserting new data, CASCADE is used to remove the foreign key constraints
    -- that way we do not repeat the data
    TRUNCATE TABLE silver.erp_loc_a101 CASCADE;
    INSERT INTO silver.erp_loc_a101 (
        cid,
        cntry
    )
    SELECT 
        REPLACE(cid, '-', '') AS cid,
        CASE WHEN TRIM(UPPER(cntry)) = 'DE' THEN 'Germany'
            WHEN TRIM(UPPER(cntry)) IN ('US', 'USA') THEN 'United States'
            WHEN TRIM(UPPER(cntry)) IS NULL OR TRIM(UPPER(cntry)) = '' THEN 'N/A'
            ELSE TRIM(UPPER(cntry))
        END AS cntry
    FROM bronze.erp_loc_a101;
    
    section_duration := clock_timestamp() - section_start;
    RAISE NOTICE 'ERP Location A101 processing completed in: %', section_duration;

    /*
    ===============================================================================
        Cleaning and inserting data into the silver.erp_px_cat_g1v2 table
    ===============================================================================
    */
    -- Section 6: Cleaning and inserting data into silver.erp_px_cat_g1v2 table
    section_start := clock_timestamp();
    RAISE NOTICE 'Starting ERP PX Category G1V2 processing...';
    
    -- truncate/remove the data before inserting new data, CASCADE is used to remove the foreign key constraints
    -- that way we do not repeat the data
    TRUNCATE TABLE silver.erp_px_cat_g1v2 CASCADE;
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
    
    section_duration := clock_timestamp() - section_start;
    RAISE NOTICE 'ERP PX Category G1V2 processing completed in: %', section_duration;
    
    -- Calculate and display total execution time
    end_time := clock_timestamp();
    total_duration := end_time - start_time;
    RAISE NOTICE '===============================================================================';
    RAISE NOTICE 'Data cleansing process completed successfully at: %', end_time;
    RAISE NOTICE 'Total execution time: %', total_duration;
    RAISE NOTICE '===============================================================================';
    
    -- Error handling
    EXCEPTION
        -- OTHERS is used to catch any error that is not explicitly caught by other EXCEPTION blocks
        WHEN OTHERS THEN
            -- SQLERRM is used to get the error message
            RAISE NOTICE 'An error occurred: %', SQLERRM;
            -- The stored procedure will automatically rollback on error
            RAISE NOTICE 'Transaction will be automatically rolled back...';
            -- RAISE is used to re-raise the error
            RAISE;
END;
$$;


CALL silver.silver_load_silver();

