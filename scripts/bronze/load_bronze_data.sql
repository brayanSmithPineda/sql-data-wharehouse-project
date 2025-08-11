/*
===============================================================================
Data Loading Script: Load Data into Bronze Tables
===============================================================================
Script Purpose:
    This script loads data from CSV files into the bronze schema tables using
    psql's \copy command (client-side copy).

Prerequisites:
    - Bronze tables must exist (run ddl_bronze.sql first)
    - CSV files must be accessible from the client machine
    - Must be executed with psql (not other SQL clients)

Usage:
    psql -h host -d database -U username -f load_bronze_data.sql
    
===============================================================================
*/

\set script_start_time `date +%s`
\echo '==============================================================================='
\echo 'Starting Bronze schema data loading process'
\echo '==============================================================================='

-- Loading data into crm_cust_info table
\echo '--------------------------------'
\echo 'Loading data into crm_cust_info table'
\set start_time `date +%s`

\copy bronze.crm_cust_info FROM '/Users/brayanpineda/Documents/Programming/General-Code/Personal Github/sql-data-warehouse-project/datasets/source_crm/cust_info.csv' WITH (FORMAT CSV, HEADER true, DELIMITER ',');

\set end_time `date +%s`
\echo 'CRM Customer Info data loaded in: ' `echo "scale=2; (:end_time - :start_time)" | bc` 'seconds'
\echo '--------------------------------'

-- Loading data into crm_prd_info table
\echo '--------------------------------'
\echo 'Loading data into crm_prd_info table'
\set start_time `date +%s`

\copy bronze.crm_prd_info FROM '/Users/brayanpineda/Documents/Programming/General-Code/Personal Github/sql-data-warehouse-project/datasets/source_crm/prd_info.csv' WITH (FORMAT CSV, HEADER true, DELIMITER ',');

\set end_time `date +%s`
\echo 'CRM Product Info data loaded in: ' `echo "scale=2; (:end_time - :start_time)" | bc` 'seconds'
\echo '--------------------------------'

-- Loading data into crm_sales_details table
\echo '--------------------------------'
\echo 'Loading data into crm_sales_details table'
\set start_time `date +%s`

\copy bronze.crm_sales_details FROM '/Users/brayanpineda/Documents/Programming/General-Code/Personal Github/sql-data-warehouse-project/datasets/source_crm/sales_details.csv' WITH (FORMAT CSV, HEADER true, DELIMITER ',');

\set end_time `date +%s`
\echo 'CRM Sales Details data loaded in: ' `echo "scale=2; (:end_time - :start_time)" | bc` 'seconds'
\echo '--------------------------------'

-- Loading data into erp_loc_a101 table
\echo '--------------------------------'
\echo 'Loading data into erp_loc_a101 table'
\set start_time `date +%s`

\copy bronze.erp_loc_a101 FROM '/Users/brayanpineda/Documents/Programming/General-Code/Personal Github/sql-data-warehouse-project/datasets/source_erp/LOC_A101.csv' WITH (FORMAT CSV, HEADER true, DELIMITER ',');

\set end_time `date +%s`
\echo 'ERP Location A101 data loaded in: ' `echo "scale=2; (:end_time - :start_time)" | bc` 'seconds'
\echo '--------------------------------'

-- Loading data into erp_cust_az12 table
\echo '--------------------------------'
\echo 'Loading data into erp_cust_az12 table'
\set start_time `date +%s`

\copy bronze.erp_cust_az12 FROM '/Users/brayanpineda/Documents/Programming/General-Code/Personal Github/sql-data-warehouse-project/datasets/source_erp/CUST_AZ12.csv' WITH (FORMAT CSV, HEADER true, DELIMITER ',');

\set end_time `date +%s`
\echo 'ERP Customer AZ12 data loaded in: ' `echo "scale=2; (:end_time - :start_time)" | bc` 'seconds'
\echo '--------------------------------'

-- Loading data into erp_px_cat_g1v2 table
\echo '--------------------------------'
\echo 'Loading data into erp_px_cat_g1v2 table'
\set start_time `date +%s`

\copy bronze.erp_px_cat_g1v2 FROM '/Users/brayanpineda/Documents/Programming/General-Code/Personal Github/sql-data-warehouse-project/datasets/source_erp/PX_CAT_G1V2.csv' WITH (FORMAT CSV, HEADER true, DELIMITER ',');

\set end_time `date +%s`
\echo 'ERP PX Category G1V2 data loaded in: ' `echo "scale=2; (:end_time - :start_time)" | bc` 'seconds'
\echo '--------------------------------'

\set script_end_time `date +%s`
\echo '==============================================================================='
\echo 'Bronze schema data loading completed successfully!'
\echo 'Total data loading time: ' `echo "scale=2; (:script_end_time - :script_start_time)" | bc` 'seconds'
\echo '==============================================================================='

-- Verify data loading with row counts
\echo ''
\echo 'Data Loading Verification:'
\echo '--------------------------------'
SELECT 'crm_cust_info' as table_name, COUNT(*) as row_count FROM bronze.crm_cust_info
UNION ALL
SELECT 'crm_prd_info' as table_name, COUNT(*) as row_count FROM bronze.crm_prd_info
UNION ALL
SELECT 'crm_sales_details' as table_name, COUNT(*) as row_count FROM bronze.crm_sales_details
UNION ALL
SELECT 'erp_loc_a101' as table_name, COUNT(*) as row_count FROM bronze.erp_loc_a101
UNION ALL
SELECT 'erp_cust_az12' as table_name, COUNT(*) as row_count FROM bronze.erp_cust_az12
UNION ALL
SELECT 'erp_px_cat_g1v2' as table_name, COUNT(*) as row_count FROM bronze.erp_px_cat_g1v2
ORDER BY table_name;