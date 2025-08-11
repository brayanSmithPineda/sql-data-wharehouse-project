/*
===============================================================================
Master Script: Complete Bronze Schema Setup
===============================================================================
Script Purpose:
    This master script executes the complete bronze schema setup process:
    1. Creates bronze tables using stored procedure
    2. Loads data from CSV files using \copy commands
    3. Provides comprehensive timing and verification

Prerequisites:
    - Bronze schema must exist
    - CSV files must be accessible from client machine
    - Must be executed with psql (not other SQL clients)

Usage:
    psql -h host -d database -U username -f run_bronze_setup.sql

Files Used:
    - Current script orchestrates the process
    - Calls stored procedure for DDL operations
    - Executes data loading operations inline

===============================================================================
*/

\set master_start_time `date +%s`
\echo '==============================================================================='
\echo 'Starting Complete Bronze Schema Setup Process'
\echo 'Time: ' `date`
\echo '==============================================================================='

\echo ''
\echo 'Step 1: Creating Bronze Tables (DDL)'
\echo '-------------------------------------'

-- Execute the DDL stored procedure
CALL bronze.create_bronze_tables();

\echo ''
\echo 'Step 2: Loading Data into Bronze Tables'
\echo '-------------------------------------'

\set data_load_start_time `date +%s`

-- Loading data into crm_cust_info table
\echo 'Loading data into crm_cust_info table...'
\set start_time `date +%s`
\copy bronze.crm_cust_info FROM '/Users/brayanpineda/Documents/Programming/General-Code/Personal Github/sql-data-warehouse-project/datasets/source_crm/cust_info.csv' WITH (FORMAT CSV, HEADER true, DELIMITER ',');
\set end_time `date +%s`
\echo '  ✓ CRM Customer Info: ' `echo "scale=2; (:end_time - :start_time)" | bc` 'seconds'

-- Loading data into crm_prd_info table
\echo 'Loading data into crm_prd_info table...'
\set start_time `date +%s`
\copy bronze.crm_prd_info FROM '/Users/brayanpineda/Documents/Programming/General-Code/Personal Github/sql-data-warehouse-project/datasets/source_crm/prd_info.csv' WITH (FORMAT CSV, HEADER true, DELIMITER ',');
\set end_time `date +%s`
\echo '  ✓ CRM Product Info: ' `echo "scale=2; (:end_time - :start_time)" | bc` 'seconds'

-- Loading data into crm_sales_details table
\echo 'Loading data into crm_sales_details table...'
\set start_time `date +%s`
\copy bronze.crm_sales_details FROM '/Users/brayanpineda/Documents/Programming/General-Code/Personal Github/sql-data-warehouse-project/datasets/source_crm/sales_details.csv' WITH (FORMAT CSV, HEADER true, DELIMITER ',');
\set end_time `date +%s`
\echo '  ✓ CRM Sales Details: ' `echo "scale=2; (:end_time - :start_time)" | bc` 'seconds'

-- Loading data into erp_loc_a101 table
\echo 'Loading data into erp_loc_a101 table...'
\set start_time `date +%s`
\copy bronze.erp_loc_a101 FROM '/Users/brayanpineda/Documents/Programming/General-Code/Personal Github/sql-data-warehouse-project/datasets/source_erp/LOC_A101.csv' WITH (FORMAT CSV, HEADER true, DELIMITER ',');
\set end_time `date +%s`
\echo '  ✓ ERP Location A101: ' `echo "scale=2; (:end_time - :start_time)" | bc` 'seconds'

-- Loading data into erp_cust_az12 table
\echo 'Loading data into erp_cust_az12 table...'
\set start_time `date +%s`
\copy bronze.erp_cust_az12 FROM '/Users/brayanpineda/Documents/Programming/General-Code/Personal Github/sql-data-warehouse-project/datasets/source_erp/CUST_AZ12.csv' WITH (FORMAT CSV, HEADER true, DELIMITER ',');
\set end_time `date +%s`
\echo '  ✓ ERP Customer AZ12: ' `echo "scale=2; (:end_time - :start_time)" | bc` 'seconds'

-- Loading data into erp_px_cat_g1v2 table
\echo 'Loading data into erp_px_cat_g1v2 table...'
\set start_time `date +%s`
\copy bronze.erp_px_cat_g1v2 FROM '/Users/brayanpineda/Documents/Programming/General-Code/Personal Github/sql-data-warehouse-project/datasets/source_erp/PX_CAT_G1V2.csv' WITH (FORMAT CSV, HEADER true, DELIMITER ',');
\set end_time `date +%s`
\echo '  ✓ ERP PX Category G1V2: ' `echo "scale=2; (:end_time - :start_time)" | bc` 'seconds'

\set data_load_end_time `date +%s`

\echo ''
\echo 'Step 3: Data Loading Verification'
\echo '-------------------------------------'
\echo 'Table Row Counts:'

SELECT 
    'crm_cust_info' as table_name, 
    COUNT(*) as row_count,
    'Customer information from CRM' as description
FROM bronze.crm_cust_info
UNION ALL
SELECT 
    'crm_prd_info' as table_name, 
    COUNT(*) as row_count,
    'Product information from CRM' as description
FROM bronze.crm_prd_info
UNION ALL
SELECT 
    'crm_sales_details' as table_name, 
    COUNT(*) as row_count,
    'Sales transaction details from CRM' as description
FROM bronze.crm_sales_details
UNION ALL
SELECT 
    'erp_loc_a101' as table_name, 
    COUNT(*) as row_count,
    'Location data from ERP system A101' as description
FROM bronze.erp_loc_a101
UNION ALL
SELECT 
    'erp_cust_az12' as table_name, 
    COUNT(*) as row_count,
    'Customer data from ERP system AZ12' as description
FROM bronze.erp_cust_az12
UNION ALL
SELECT 
    'erp_px_cat_g1v2' as table_name, 
    COUNT(*) as row_count,
    'Product category data from ERP G1V2' as description
FROM bronze.erp_px_cat_g1v2
ORDER BY table_name;

\set master_end_time `date +%s`

\echo ''
\echo '==============================================================================='
\echo 'Bronze Schema Setup Completed Successfully!'
\echo '==============================================================================='
\echo 'Summary:'
\echo '  • Tables Created: 6'
\echo '  • Data Loading Time: ' `echo "scale=2; (:data_load_end_time - :data_load_start_time)" | bc` 'seconds'
\echo '  • Total Setup Time: ' `echo "scale=2; (:master_end_time - :master_start_time)" | bc` 'seconds'
\echo '  • Completion Time: ' `date`
\echo '==============================================================================='
\echo ''
\echo 'Next Steps:'
\echo '  1. Verify data quality in bronze schema'
\echo '  2. Run silver layer data cleansing procedure'
\echo '  3. Execute gold layer analytics transformations'
\echo '==============================================================================='