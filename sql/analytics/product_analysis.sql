-- Product Performance Analysis
WITH product_metrics AS (
    SELECT 
        p.product_id,
        p.product_name,
        p.category,
        p.subcategory,
        COUNT(DISTINCT s.sales_id) as total_sales,
        SUM(s.quantity) as total_quantity_sold,
        SUM(s.total_amount) as total_revenue,
        SUM(s.quantity * p.unit_price - s.total_amount) as total_discount_given,
        AVG(i.closing_stock) as avg_stock_level
    FROM dim_product p
    LEFT JOIN fact_sales s ON p.product_id = s.product_id
    LEFT JOIN fact_inventory i ON p.product_id = i.product_id
    GROUP BY p.product_id, p.product_name, p.category, p.subcategory
)
SELECT 
    *,
    CASE 
        WHEN total_quantity_sold > 1000 AND total_revenue > 10000 THEN 'High Performer'
        WHEN total_quantity_sold > 500 AND total_revenue > 5000 THEN 'Good Performer'
        WHEN total_quantity_sold > 0 THEN 'Low Performer'
        ELSE 'No Sales'
    END as product_performance_category
FROM product_metrics
ORDER BY total_revenue DESC;
