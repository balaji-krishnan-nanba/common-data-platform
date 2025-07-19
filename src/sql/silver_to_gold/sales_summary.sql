-- Sales Summary Transformation
-- Creates daily, weekly, and monthly sales aggregations
-- Source: silver.excel_data.daily_sales

WITH daily_aggregates AS (
    SELECT 
        transaction_date,
        YEAR(transaction_date) AS year,
        MONTH(transaction_date) AS month,
        WEEKOFYEAR(transaction_date) AS week_of_year,
        customer_id,
        product_code,
        store_location,
        payment_method,
        
        -- Sales metrics
        SUM(quantity) AS total_quantity,
        SUM(total_amount) AS total_sales_amount,
        AVG(unit_price) AS avg_unit_price,
        COUNT(DISTINCT transaction_id) AS transaction_count,
        
        -- Transaction analysis
        MIN(total_amount) AS min_transaction_amount,
        MAX(total_amount) AS max_transaction_amount,
        
        CURRENT_TIMESTAMP() AS processed_at
        
    FROM ${project_code}-${environment}-silver.excel_data.daily_sales
    WHERE transaction_date IS NOT NULL
        AND total_amount > 0
        AND quantity > 0
    GROUP BY 
        transaction_date,
        YEAR(transaction_date),
        MONTH(transaction_date),
        WEEKOFYEAR(transaction_date),
        customer_id,
        product_code,
        store_location,
        payment_method
),

enriched_sales AS (
    SELECT 
        *,
        
        -- Business insights
        CASE 
            WHEN total_sales_amount >= 1000 THEN 'High Value'
            WHEN total_sales_amount >= 100 THEN 'Medium Value'
            ELSE 'Low Value'
        END AS transaction_value_category,
        
        CASE 
            WHEN payment_method = 'Credit Card' THEN 'Digital'
            WHEN payment_method = 'Cash' THEN 'Traditional'
            ELSE 'Other'
        END AS payment_type_category,
        
        -- Store performance indicators
        total_sales_amount / NULLIF(transaction_count, 0) AS avg_transaction_value,
        
        ROW_NUMBER() OVER (
            PARTITION BY transaction_date, store_location 
            ORDER BY total_sales_amount DESC
        ) AS daily_store_rank,
        
        '${environment}' AS source_environment
        
    FROM daily_aggregates
)

SELECT * FROM enriched_sales
ORDER BY transaction_date DESC, total_sales_amount DESC