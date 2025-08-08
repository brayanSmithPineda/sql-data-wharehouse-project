/*
===============================================================================
PostgreSQL Function: Create Bronze Tables
===============================================================================
Script Purpose:
    This function creates all bronze schema tables (DDL only).
    Data loading must be done separately using \copy in psql.

Usage:
    1. Run this script to create the function
    2. Call: SELECT bronze.create_bronze_tables();
    3. Then run \copy commands in psql for data loading

Note: PostgreSQL uses FUNCTION, not PROCEDURE
===============================================================================
*/

CREATE OR REPLACE FUNCTION bronze.create_bronze_tables()
RETURNS TEXT -- in this case, we return a simple text message after execution
LANGUAGE plpgsql
AS $$
BEGIN
    -- Create bronze schema if it doesn't exist
    CREATE SCHEMA IF NOT EXISTS bronze;
    
    -- Drop existing tables if they exist
    -- CASCADE is used to drop dependent objects
    DROP TABLE IF EXISTS bronze.crm_cust_info CASCADE;
    DROP TABLE IF EXISTS bronze.crm_prd_info CASCADE;
    DROP TABLE IF EXISTS bronze.crm_sales_details CASCADE;
    DROP TABLE IF EXISTS bronze.erp_loc_a101 CASCADE;
    DROP TABLE IF EXISTS bronze.erp_cust_az12 CASCADE;
    DROP TABLE IF EXISTS bronze.erp_px_cat_g1v2 CASCADE;
    
    -- Create bronze tables
    CREATE TABLE bronze.crm_cust_info (
        cst_id              INT,
        cst_key             VARCHAR(50),
        cst_firstname       VARCHAR(50),
        cst_lastname        VARCHAR(50),
        cst_marital_status  VARCHAR(50),
        cst_gndr            VARCHAR(50),
        cst_create_date     DATE
    );

    CREATE TABLE bronze.crm_prd_info (
        prd_id       INT,
        prd_key      VARCHAR(50),
        prd_nm       VARCHAR(50),
        prd_cost     INT,
        prd_line     VARCHAR(50),
        prd_start_dt TIMESTAMP,
        prd_end_dt   TIMESTAMP
    );

    CREATE TABLE bronze.crm_sales_details (
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

    CREATE TABLE bronze.erp_loc_a101 (
        cid    VARCHAR(50),
        cntry  VARCHAR(50)
    );

    CREATE TABLE bronze.erp_cust_az12 (
        cid    VARCHAR(50),
        bdate  DATE,
        gen    VARCHAR(50)
    );

    CREATE TABLE bronze.erp_px_cat_g1v2 (
        id           VARCHAR(50),
        cat          VARCHAR(50),
        subcat       VARCHAR(50),
        maintenance  VARCHAR(50)
    );

    RETURN 'Bronze tables created successfully';

EXCEPTION
    WHEN OTHERS THEN
        RETURN 'ERROR: Failed to create bronze tables - ' || SQLERRM || ' (Error Code: ' || SQLSTATE || ')';
END;
$$;

-- To execute the function and create the tables, run:
SELECT bronze.create_bronze_tables();
