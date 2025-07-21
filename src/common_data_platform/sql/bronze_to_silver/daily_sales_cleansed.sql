-- Clean and validate daily sales data from bronze to silver layer
-- Removes invalid records, standardizes data types, and adds business logic

CREATE OR REPLACE TABLE ${target_catalog}.${target_schema}.${target_table}
USING DELTA
PARTITIONED BY (transaction_date)
AS
SELECT 
    -- Core transaction data
    TRIM(transaction_id) as transaction_id,
    transaction_date,
    TRIM(UPPER(customer_id)) as customer_id,
    TRIM(UPPER(product_code)) as product_code,
    
    -- Validated numeric fields
    quantity,
    unit_price,
    total_amount,
    
    -- Calculated fields
    ROUND(quantity * unit_price, 2) as calculated_amount,
    CASE 
        WHEN ABS(total_amount - (quantity * unit_price)) > 0.01 
        THEN 'AMOUNT_MISMATCH'
        ELSE 'VALID'
    END as data_quality_flag,
    
    -- Standardized text fields
    CASE 
        WHEN TRIM(UPPER(payment_method)) IN ('CASH', 'CARD', 'CREDIT', 'DEBIT', 'CHECK') 
        THEN TRIM(UPPER(payment_method))
        ELSE 'OTHER'
    END as payment_method,
    
    TRIM(store_location) as store_location,
    
    -- Audit columns from bronze
    source_system,
    source_file,
    batch_id,
    ingestion_timestamp,
    
    -- Silver layer metadata
    current_timestamp() as silver_processed_timestamp,
    '${environment}' as environment
    
FROM ${source_catalog}.${source_schema}.${source_table}

WHERE 
    -- Data quality filters
    transaction_id IS NOT NULL
    AND transaction_date IS NOT NULL
    AND customer_id IS NOT NULL
    AND product_code IS NOT NULL
    AND quantity > 0
    AND unit_price >= 0
    AND total_amount >= 0
    AND transaction_date >= '2020-01-01'  -- Reasonable date range
    AND transaction_date <= current_date()