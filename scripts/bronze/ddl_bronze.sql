/*
===============================================================================
DDL Script: Create Bronze Tables
===============================================================================
Script Purpose:
    This script creates tables in the 'bronze' schema, dropping existing tables 
    if they already exist.
	  Run this script to re-define the DDL structure of 'bronze' Tables

    You should run this script with psql
===============================================================================
*/

-- creating and inserting data into the crm_cust_info table
\set script_start_time `date +%s`
\echo '--------------------------------'
\echo 'Creating and inserting data into the crm_cust_info table'
\set start_time `date +%s`
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
\copy bronze.crm_cust_info FROM '/Users/brayanpineda/Documents/Programming/General-Code/Personal Github/sql-data-warehouse-project/datasets/source_crm/cust_info.csv' WITH (FORMAT CSV, HEADER true, DELIMITER ',');

\set end_time `date +%s`
\echo 'Time taken: ' `echo "scale=2; (:end_time - :start_time)" | bc` 'seconds'
\echo '--------------------------------'

-- creating the crm_prd_info table
\echo '--------------------------------'
\echo 'Creating and inserting data into the crm_prd_info table'
\set start_time `date +%s`
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
\copy bronze.crm_prd_info FROM '/Users/brayanpineda/Documents/Programming/General-Code/Personal Github/sql-data-warehouse-project/datasets/source_crm/prd_info.csv' WITH (FORMAT CSV, HEADER true, DELIMITER ',');

\set end_time `date +%s`
\echo 'Time taken: ' `echo "scale=2; (:end_time - :start_time)" | bc` 'seconds'
\echo '--------------------------------'

-- creating the crm_sales_details table
\echo '--------------------------------'
\echo 'Creating and inserting data into the crm_sales_details table'
\set start_time `date +%s`
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
\copy bronze.crm_sales_details FROM '/Users/brayanpineda/Documents/Programming/General-Code/Personal Github/sql-data-warehouse-project/datasets/source_crm/sales_details.csv' WITH (FORMAT CSV, HEADER true, DELIMITER ',');

\set end_time `date +%s`
\echo 'Time taken: ' `echo "scale=2; (:end_time - :start_time)" | bc` 'seconds'
\echo '--------------------------------'


-- creating the erp_loc_a101 table
\echo '--------------------------------'
\echo 'Creating and inserting data into the erp_loc_a101 table'
\set start_time `date +%s`
CREATE TABLE IF NOT EXISTS bronze.erp_loc_a101 (
    cid    VARCHAR(50),
    cntry  VARCHAR(50)
);

TRUNCATE TABLE bronze.erp_loc_a101;
\copy bronze.erp_loc_a101 FROM '/Users/brayanpineda/Documents/Programming/General-Code/Personal Github/sql-data-warehouse-project/datasets/source_erp/LOC_A101.csv' WITH (FORMAT CSV, HEADER true, DELIMITER ',');

\set end_time `date +%s`
\echo 'Time taken: ' `echo "scale=2; (:end_time - :start_time)" | bc` 'seconds'
\echo '--------------------------------'


-- creating the erp_cust_az12 table
\echo '--------------------------------'
\echo 'Creating and inserting data into the erp_cust_az12 table'
\set start_time `date +%s`
CREATE TABLE IF NOT EXISTS bronze.erp_cust_az12 (
    cid    VARCHAR(50),
    bdate  DATE,
    gen    VARCHAR(50)
);

TRUNCATE TABLE bronze.erp_cust_az12;
\copy bronze.erp_cust_az12 FROM '/Users/brayanpineda/Documents/Programming/General-Code/Personal Github/sql-data-warehouse-project/datasets/source_erp/CUST_AZ12.csv' WITH (FORMAT CSV, HEADER true, DELIMITER ',');

\set end_time `date +%s`
\echo 'Time taken: ' `echo "scale=2; (:end_time - :start_time)" | bc` 'seconds'
\echo '--------------------------------'


-- creating the erp_px_cat_g1v2 table
\echo '--------------------------------'
\echo 'Creating and inserting data into the erp_px_cat_g1v2 table'
\set start_time `date +%s`
CREATE TABLE IF NOT EXISTS bronze.erp_px_cat_g1v2 (
    id           VARCHAR(50),
    cat          VARCHAR(50),
    subcat       VARCHAR(50),
    maintenance  VARCHAR(50)
);

TRUNCATE TABLE bronze.erp_px_cat_g1v2;
\copy bronze.erp_px_cat_g1v2 FROM '/Users/brayanpineda/Documents/Programming/General-Code/Personal Github/sql-data-warehouse-project/datasets/source_erp/PX_CAT_G1V2.csv' WITH (FORMAT CSV, HEADER true, DELIMITER ',');

\set end_time `date +%s`
\echo 'Time taken: ' `echo "scale=2; (:end_time - :start_time)" | bc` 'seconds'
\echo '--------------------------------'

\set script_end_time `date +%s`
\echo '--------------------------------'
\echo 'Total time taken: ' `echo "scale=2; (:script_end_time - :script_start_time)" | bc` 'seconds'
\echo '--------------------------------'




