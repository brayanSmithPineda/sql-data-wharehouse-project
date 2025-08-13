-- Changes over time (Helps tracks trends and seasonality)

-- 1. Total sales over time (yearly)
SELECT 
    EXTRACT(YEAR FROM order_date) AS year,
    SUM(sales_amount) AS total_sales,
    COUNT(DISTINCT customer_key) AS total_customers,
    SUM(quantity) AS total_quantity
FROM gold.fact_sales
WHERE order_date IS NOT NULL
GROUP BY year
ORDER BY year DESC

-- Cumulative analysis (Aggregate the data progressively over time, helps us to see if our buisness is growing or shrinking)

-- Calculate the running total of sales yearly
SELECT 
    month,
    total_sales,
    SUM(total_sales) OVER(PARTITION BY year ORDER BY month) AS running_total_sales
FROM (
    SELECT 
        EXTRACT(YEAR FROM order_date) AS year,
        DATE_TRUNC('month', order_date) AS month,
        SUM(sales_amount) AS total_sales
    FROM gold.fact_sales
    WHERE order_date IS NOT NULL
    GROUP BY DATE_TRUNC('month', order_date), EXTRACT(YEAR FROM order_date) 
)

-- Performace analysis (Compara the current value and the target value, this helps us to see if we are meeting our goals)

-- Analyze the yearly performace of products by comparing each product's sales to both is avarege sales performace and
-- previous year's sales performace
WITH product_sales AS (
    SELECT 
        EXTRACT(YEAR FROM order_date) AS year,
        product_name,
        SUM(sales_amount) AS current_year_sales
    FROM gold.fact_sales s
    LEFT JOIN gold.dim_products p ON s.product_key = p.product_key
    WHERE order_date IS NOT NULL
    GROUP BY EXTRACT(YEAR FROM order_date), p.product_name
)
SELECT 
    year,
    product_name,
    current_year_sales,
    AVG(current_year_sales) OVER(PARTITION BY product_name) AS avg_sales,
    current_year_sales - AVG(current_year_sales) OVER(PARTITION BY product_name) AS diff_from_avg,
    LAG(current_year_sales) OVER(PARTITION BY product_name ORDER BY year) AS prev_year_sales,
    current_year_sales - LAG(current_year_sales) OVER(PARTITION BY product_name ORDER BY year) AS diff_from_prev_year
FROM product_sales
ORDER BY product_name ASC, year ASC

-- Part to whole analysis (How an individual part contributes to the whole)

--which category contributes the most to the total sales
SELECT 
    category,
    total_sales,
    ROUND(total_sales / SUM(total_sales) OVER() * 100, 2) AS category_percentage
FROM (
    SELECT 
        p.category,
        SUM(s.sales_amount) AS total_sales
    FROM gold.fact_sales s
    LEFT JOIN gold.dim_products p ON s.product_key = p.product_key
    GROUP BY p.category
)



-- Data segmentation (group data into specific range)

--Segment products into cost ranges and count the products in each range
SELECT 
    CASE 
        WHEN cost < 100 THEN 'Low Cost'
        WHEN cost BETWEEN 100 AND 500 THEN 'Medium Cost'
        ELSE 'High Cost'
    END AS cost_range,
    COUNT(*) AS product_count
FROM gold.dim_products
GROUP BY cost_range

SELECT *
FROM gold.dim_products

-- Build customer report