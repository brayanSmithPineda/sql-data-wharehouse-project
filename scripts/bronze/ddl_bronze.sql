/*
===============================================================================
DDL Script: Create Bronze Tables and Load Data
===============================================================================
Script Purpose:
    This script creates tables in the 'bronze' schema and loads data from CSV files
    using a stored procedure with COPY FROM commands.

    Requirements:
    - User needs pg_read_server_files role or superuser privileges
    - CSV files must be accessible by PostgreSQL server

===============================================================================
*/

CREATE OR REPLACE PROCEDURE bronze.create_bronze_tables()
LANGUAGE plpgsql
AS $$
DECLARE
    start_time TIMESTAMP;
    section_start TIMESTAMP;
    section_duration INTERVAL;
    total_duration INTERVAL;
BEGIN
    start_time := clock_timestamp();
    RAISE NOTICE 'Starting Bronze schema table creation at: %', start_time;
    
    -- Section 1: Creating crm_cust_info table
    section_start := clock_timestamp();
    RAISE NOTICE 'Creating crm_cust_info table...';
    CREATE TABLE IF NOT EXISTS bronze.crm_cust_info (
        cst_id              INT,
        cst_key             VARCHAR(50),
        cst_firstname       VARCHAR(50),
        cst_lastname        VARCHAR(50),
        cst_marital_status  VARCHAR(50),
        cst_gndr            VARCHAR(50),
        cst_create_date     DATE
    );

    TRUNCATE TABLE bronze.crm_cust_info;

    section_duration := clock_timestamp() - section_start;
    RAISE NOTICE 'CRM Customer Info table created in: %', section_duration;

    -- Section 2: Creating crm_prd_info table
    section_start := clock_timestamp();
    RAISE NOTICE 'Creating crm_prd_info table...';
    
    CREATE TABLE IF NOT EXISTS bronze.crm_prd_info (
        prd_id       INT,
        prd_key      VARCHAR(50),
        prd_nm       VARCHAR(50),
        prd_cost     INT,
        prd_line     VARCHAR(50),
        prd_start_dt TIMESTAMP,
        prd_end_dt   TIMESTAMP
    );

    TRUNCATE TABLE bronze.crm_prd_info;

    section_duration := clock_timestamp() - section_start;
    RAISE NOTICE 'CRM Product Info table created in: %', section_duration;

    -- Section 3: Creating crm_sales_details table
    section_start := clock_timestamp();
    RAISE NOTICE 'Creating crm_sales_details table...';
    
    CREATE TABLE IF NOT EXISTS bronze.crm_sales_details (
        sls_ord_num  VARCHAR(50),
        sls_prd_key  VARCHAR(50),
        sls_cust_id  INT,
        sls_order_dt INT,
        sls_ship_dt  INT,
        sls_due_dt   INT,
        sls_sales    INT,
        sls_quantity INT,
        sls_price    INT
    );

    TRUNCATE TABLE bronze.crm_sales_details;

    section_duration := clock_timestamp() - section_start;
    RAISE NOTICE 'CRM Sales Details table created in: %', section_duration;


    -- Section 4: Creating erp_loc_a101 table
    section_start := clock_timestamp();
    RAISE NOTICE 'Creating erp_loc_a101 table...';
    
    CREATE TABLE IF NOT EXISTS bronze.erp_loc_a101 (
        cid    VARCHAR(50),
        cntry  VARCHAR(50)
    );

    TRUNCATE TABLE bronze.erp_loc_a101;

    section_duration := clock_timestamp() - section_start;
    RAISE NOTICE 'ERP Location A101 table created in: %', section_duration;


    -- Section 5: Creating erp_cust_az12 table
    section_start := clock_timestamp();
    RAISE NOTICE 'Creating erp_cust_az12 table...';
    
    CREATE TABLE IF NOT EXISTS bronze.erp_cust_az12 (
        cid    VARCHAR(50),
        bdate  DATE,
        gen    VARCHAR(50)
    );

    TRUNCATE TABLE bronze.erp_cust_az12;

    section_duration := clock_timestamp() - section_start;
    RAISE NOTICE 'ERP Customer AZ12 table created in: %', section_duration;


    -- Section 6: Creating erp_px_cat_g1v2 table
    section_start := clock_timestamp();
    RAISE NOTICE 'Creating erp_px_cat_g1v2 table...';
    
    CREATE TABLE IF NOT EXISTS bronze.erp_px_cat_g1v2 (
        id           VARCHAR(50),
        cat          VARCHAR(50),
        subcat       VARCHAR(50),
        maintenance  VARCHAR(50)
    );

    TRUNCATE TABLE bronze.erp_px_cat_g1v2;

    section_duration := clock_timestamp() - section_start;
    RAISE NOTICE 'ERP PX Category G1V2 table created in: %', section_duration;
    
    -- Calculate and display total execution time
    total_duration := clock_timestamp() - start_time;
    RAISE NOTICE '===============================================================================';
    RAISE NOTICE 'Bronze schema table creation completed successfully!';
    RAISE NOTICE 'Total execution time: %', total_duration;
    RAISE NOTICE 'Tables are ready for data loading.';
    RAISE NOTICE '===============================================================================';
    
EXCEPTION
    WHEN OTHERS THEN
        RAISE NOTICE 'An error occurred: %', SQLERRM;
        RAISE NOTICE 'Transaction will be automatically rolled back...';
        RAISE;
END;
$$;

-- Execute the DDL procedure
CALL bronze.create_bronze_tables();


