/*
    Customer Data Model
*/
CREATE OR REPLACE VIEW gold.dim_customers AS
SELECT 
    ROW_NUMBER() OVER (ORDER BY ci.cst_id) AS customer_key, --surrogate key to easily combine the fact table with the dimension table
    ci.cst_id AS customer_id,
    ci.cst_key AS customer_number,
    ci.cst_firstname AS first_name,
    ci.cst_lastname AS last_name,
    el.cntry AS country,
    ci.cst_marital_status AS marital_status,
    CASE 
        WHEN ci.cst_gndr != 'N/A' THEN ci.cst_gndr 
        ELSE COALESCE(caz.gen, 'N/A') 
    END AS gender,
    caz.bdate AS birthdate,
    ci.cst_create_date AS create_date
    
FROM silver.crm_cust_info ci -- master table
LEFT JOIN silver.erp_cust_az12 caz ON ci.cst_key = caz.cid
LEFT JOIN silver.erp_loc_a101 el ON ci.cst_key = el.cid
;

/*
    Product Data Model
*/
CREATE OR REPLACE VIEW gold.dim_products AS
SELECT 
    ROW_NUMBER() OVER (ORDER BY pi.prd_start_dt, pi.prd_key) AS product_key, --surrogate key to easily combine the fact table with the dimension table
    pi.prd_id AS product_id,
    pi.prd_key AS product_number,
    pi.prd_nm AS product_name,
    pi.cat_id AS category_id,
    pxc.cat AS category,
    pxc.subcat As subcategory,
    pxc.maintenance,
    pi.prd_cost AS cost,
    pi.prd_line AS product_line,
    pi.prd_start_dt AS start_date
FROM silver.crm_prd_info AS pi
LEFT JOIN silver.erp_px_cat_g1v2 AS pxc ON pi.cat_id = pxc.id
WHERE pi.prd_end_dt IS NULL -- We only want the current products. No history.
/*
    Sales Data Model: Fact table
*/
CREATE OR REPLACE VIEW gold.fact_sales AS
SELECT
    sd.sls_ord_num AS order_number,
    p.product_key,
    c.customer_key,
    sd.sls_order_dt AS order_date,
    sd.sls_ship_dt AS ship_date,
    sd.sls_due_dt AS due_date,
    sd.sls_sales AS sales_amount,
    sd.sls_quantity AS quantity,
    sd.sls_price AS price
FROM silver.crm_sales_details AS sd
LEFT JOIN gold.dim_customers c ON sd.sls_cust_id = c.customer_id
LEFT JOIN gold.dim_products p ON sd.sls_prd_key = p.product_number

SELECT *
FROM gold.fact_sales
LIMIT 10;

