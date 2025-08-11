/*
===============================================================================
PostgreSQL Script: DDL for Silver Schema
===============================================================================
Script Purpose:
    This script creates the tables in the silver schema. we not insert data yet.
Note:
    1- Basically the same tables as the bronze schema tables, we sort of like initlize them before
    we start cleaning the data.
    2- We include dwh_create_date column with system timestamp when the row was inserted
===============================================================================
*/

-- creating and inserting data into the crm_cust_info table
\set script_start_time `date +%s`
\echo '--------------------------------'
\echo 'Creating and inserting data into the crm_cust_info table'
\set start_time `date +%s`
CREATE TABLE IF NOT EXISTS silver.crm_cust_info (
    cst_id              INT,
    cst_key             VARCHAR(50),
    cst_firstname       VARCHAR(50),
    cst_lastname        VARCHAR(50),
    cst_marital_status  VARCHAR(50),
    cst_gndr            VARCHAR(50),
    cst_create_date     DATE,
    dwh_create_date     DATE DEFAULT NOW() -- data wharehouse column with system timestamp when the row was inserted
);
\set end_time `date +%s`
\echo 'Time taken: ' `echo "scale=2; (:end_time - :start_time)" | bc` 'seconds'
\echo '--------------------------------'

-- creating the crm_prd_info table
\echo '--------------------------------'
\echo 'Creating and inserting data into the crm_prd_info table'
\set start_time `date +%s`

CREATE TABLE IF NOT EXISTS silver.crm_prd_info (
    prd_id       INT,
    cat_id       VARCHAR(50),
    prd_key      VARCHAR(50),
    prd_nm       VARCHAR(50),
    prd_cost     INT,
    prd_line     VARCHAR(50),
    prd_start_dt DATE,
    prd_end_dt   DATE,
    dwh_create_date DATE DEFAULT NOW() -- data wharehouse column with system timestamp when the row was inserted
);

\set end_time `date +%s`
\echo 'Time taken: ' `echo "scale=2; (:end_time - :start_time)" | bc` 'seconds'
\echo '--------------------------------'

-- creating the crm_sales_details table
\echo '--------------------------------'
\echo 'Creating and inserting data into the crm_sales_details table'
\set start_time `date +%s`
DROP TABLE IF EXISTS silver.crm_sales_details;
CREATE TABLE IF NOT EXISTS silver.crm_sales_details (
    sls_ord_num  VARCHAR(50),
    sls_prd_key  VARCHAR(50),
    sls_cust_id  INT,
    sls_order_dt DATE,
    sls_ship_dt  DATE,
    sls_due_dt   DATE,
    sls_sales    INT,
    sls_quantity INT,
    sls_price    INT,
    dwh_create_date     DATE DEFAULT NOW() -- data wharehouse column with system timestamp when the row was inserted
);

\set end_time `date +%s`
\echo 'Time taken: ' `echo "scale=2; (:end_time - :start_time)" | bc` 'seconds'
\echo '--------------------------------'


-- creating the erp_loc_a101 table
\echo '--------------------------------'
\echo 'Creating and inserting data into the erp_loc_a101 table'
\set start_time `date +%s`
CREATE TABLE IF NOT EXISTS silver.erp_loc_a101 (
    cid    VARCHAR(50),
    cntry  VARCHAR(50),
    dwh_create_date     DATE DEFAULT NOW() -- data wharehouse column with system timestamp when the row was inserted
);

\set end_time `date +%s`
\echo 'Time taken: ' `echo "scale=2; (:end_time - :start_time)" | bc` 'seconds'
\echo '--------------------------------'


-- creating the erp_cust_az12 table
\echo '--------------------------------'
\echo 'Creating and inserting data into the erp_cust_az12 table'
\set start_time `date +%s`
CREATE TABLE IF NOT EXISTS silver.erp_cust_az12 (
    cid    VARCHAR(50),
    bdate  DATE,
    gen    VARCHAR(50),
    dwh_create_date     DATE DEFAULT NOW() -- data wharehouse column with system timestamp when the row was inserted
);

\set end_time `date +%s`
\echo 'Time taken: ' `echo "scale=2; (:end_time - :start_time)" | bc` 'seconds'
\echo '--------------------------------'


-- creating the erp_px_cat_g1v2 table
\echo '--------------------------------'
\echo 'Creating and inserting data into the erp_px_cat_g1v2 table'
\set start_time `date +%s`
CREATE TABLE IF NOT EXISTS silver.erp_px_cat_g1v2 (
    id           VARCHAR(50),
    cat          VARCHAR(50),
    subcat       VARCHAR(50),
    maintenance  VARCHAR(50),
    dwh_create_date     DATE DEFAULT NOW() -- data wharehouse column with system timestamp when the row was inserted
);

\set end_time `date +%s`
\echo 'Time taken: ' `echo "scale=2; (:end_time - :start_time)" | bc` 'seconds'
\echo '--------------------------------'

\set script_end_time `date +%s`
\echo '--------------------------------'
\echo 'Total time taken: ' `echo "scale=2; (:script_end_time - :script_start_time)" | bc` 'seconds'
\echo '--------------------------------'
