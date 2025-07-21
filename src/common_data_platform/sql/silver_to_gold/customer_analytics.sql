-- Customer Analytics Transformation
-- Creates aggregated customer metrics for business analytics
-- Source: silver.excel_data.customer_master

SELECT 
    customer_id,
    customer_name,
    email,
    phone,
    city,
    state,
    customer_segment,
    signup_date,
    is_active,
    
    -- Customer tenure metrics
    DATEDIFF(CURRENT_DATE(), signup_date) AS days_since_signup,
    CASE 
        WHEN DATEDIFF(CURRENT_DATE(), signup_date) <= 30 THEN 'New'
        WHEN DATEDIFF(CURRENT_DATE(), signup_date) <= 365 THEN 'Regular'
        ELSE 'Veteran'
    END AS customer_tenure_category,
    
    -- Geographic grouping
    CASE 
        WHEN state IN ('CA', 'OR', 'WA') THEN 'West Coast'
        WHEN state IN ('NY', 'NJ', 'CT', 'MA') THEN 'Northeast'
        WHEN state IN ('TX', 'FL', 'GA', 'NC') THEN 'South'
        ELSE 'Other'
    END AS geographic_region,
    
    -- Data quality indicators
    CASE 
        WHEN email IS NOT NULL AND phone IS NOT NULL THEN 'Complete'
        WHEN email IS NOT NULL OR phone IS NOT NULL THEN 'Partial'
        ELSE 'Minimal'
    END AS contact_completeness,
    
    -- Metadata
    CURRENT_TIMESTAMP() AS processed_at,
    '${environment}' AS source_environment
    
FROM ${project_code}-${environment}-silver.excel_data.customer_master
WHERE is_active = true
    AND customer_name IS NOT NULL
    AND signup_date IS NOT NULL