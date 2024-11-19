WITH ProductNameQualityCheck AS (
    SELECT 
        CASE 
            WHEN product_name IS NULL OR TRIM(product_name) = '' THEN 'Fail'
            ELSE 'Pass'
        END AS ProductNameCheckResult
    FROM 
        products
)
SELECT * FROM ProductNameQualityCheck
RankedProducts AS (
    SELECT 
        product_id,
        product_name,
        total_quantity_sold,
        ROW_NUMBER() OVER (ORDER BY total_quantity_sold DESC) AS rank
    FROM 
        SalesData
)
SELECT 
    product_id,
    product_name,
    total_quantity_sold
FROM 
    RankedProducts
WHERE 
    rank <= 5;