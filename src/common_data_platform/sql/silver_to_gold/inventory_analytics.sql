-- Inventory Analytics Transformation
-- Combines product catalog, inventory movements, and stock levels for comprehensive inventory insights
-- Sources: silver.csv_data.product_catalog, silver.csv_data.inventory_movements, silver.oracle_data.inventory

WITH current_stock AS (
    SELECT 
        product_id,
        warehouse_id,
        SUM(CASE WHEN movement_type = 'IN' THEN quantity ELSE 0 END) AS total_in,
        SUM(CASE WHEN movement_type = 'OUT' THEN quantity ELSE 0 END) AS total_out,
        SUM(CASE WHEN movement_type = 'IN' THEN quantity 
                 WHEN movement_type = 'OUT' THEN -quantity 
                 ELSE 0 END) AS current_stock_level,
        MAX(movement_date) AS last_movement_date,
        COUNT(*) AS total_movements
    FROM ${project_code}-${environment}-silver.csv_data.inventory_movements
    WHERE movement_date >= DATEADD(month, -12, CURRENT_DATE())
    GROUP BY product_id, warehouse_id
),

product_enriched AS (
    SELECT 
        p.product_id,
        p.product_name,
        p.category,
        p.subcategory,
        p.brand,
        p.unit_price,
        p.cost_price,
        p.supplier_id,
        p.is_active,
        
        -- Profitability metrics
        (p.unit_price - p.cost_price) AS unit_margin,
        (p.unit_price - p.cost_price) / NULLIF(p.unit_price, 0) * 100 AS margin_percentage,
        
        -- Product age
        DATEDIFF(CURRENT_DATE(), p.created_date) AS product_age_days,
        CASE 
            WHEN DATEDIFF(CURRENT_DATE(), p.created_date) <= 90 THEN 'New'
            WHEN DATEDIFF(CURRENT_DATE(), p.created_date) <= 365 THEN 'Established'
            ELSE 'Mature'
        END AS product_lifecycle_stage
        
    FROM ${project_code}-${environment}-silver.csv_data.product_catalog p
    WHERE p.is_active = true
),

inventory_summary AS (
    SELECT 
        pe.*,
        cs.warehouse_id,
        COALESCE(cs.current_stock_level, 0) AS current_stock_level,
        COALESCE(cs.total_in, 0) AS total_inbound_movements,
        COALESCE(cs.total_out, 0) AS total_outbound_movements,
        cs.last_movement_date,
        COALESCE(cs.total_movements, 0) AS movement_frequency,
        
        -- Inventory value calculations
        COALESCE(cs.current_stock_level, 0) * pe.cost_price AS inventory_cost_value,
        COALESCE(cs.current_stock_level, 0) * pe.unit_price AS inventory_retail_value,
        COALESCE(cs.current_stock_level, 0) * pe.unit_margin AS potential_profit,
        
        -- Stock status indicators
        CASE 
            WHEN COALESCE(cs.current_stock_level, 0) = 0 THEN 'Out of Stock'
            WHEN COALESCE(cs.current_stock_level, 0) <= 10 THEN 'Low Stock'
            WHEN COALESCE(cs.current_stock_level, 0) <= 50 THEN 'Medium Stock'
            ELSE 'High Stock'
        END AS stock_status,
        
        -- Movement velocity
        CASE 
            WHEN cs.last_movement_date IS NULL THEN 'No Movement'
            WHEN DATEDIFF(CURRENT_DATE(), cs.last_movement_date) <= 7 THEN 'Fast Moving'
            WHEN DATEDIFF(CURRENT_DATE(), cs.last_movement_date) <= 30 THEN 'Regular Moving'
            ELSE 'Slow Moving'
        END AS movement_velocity,
        
        -- Days since last movement
        COALESCE(DATEDIFF(CURRENT_DATE(), cs.last_movement_date), 9999) AS days_since_last_movement,
        
        CURRENT_TIMESTAMP() AS processed_at,
        '${environment}' AS source_environment
        
    FROM product_enriched pe
    LEFT JOIN current_stock cs ON pe.product_id = cs.product_id
)

SELECT 
    product_id,
    product_name,
    category,
    subcategory,
    brand,
    unit_price,
    cost_price,
    unit_margin,
    margin_percentage,
    supplier_id,
    warehouse_id,
    current_stock_level,
    inventory_cost_value,
    inventory_retail_value,
    potential_profit,
    stock_status,
    movement_velocity,
    days_since_last_movement,
    total_inbound_movements,
    total_outbound_movements,
    movement_frequency,
    last_movement_date,
    product_age_days,
    product_lifecycle_stage,
    processed_at,
    source_environment
    
FROM inventory_summary
ORDER BY 
    inventory_retail_value DESC,
    product_name