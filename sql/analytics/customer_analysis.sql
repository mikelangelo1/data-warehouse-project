-- Customer Segmentation by Purchase Frequency and Value
WITH customer_metrics AS (
    SELECT 
        c.customer_id,
        c.first_name,
        c.last_name,
        COUNT(DISTINCT s.sales_id) as purchase_count,
        SUM(s.total_amount) as total_spend,
        AVG(s.total_amount) as avg_transaction_value,
        MAX(d.full_date) as last_purchase_date,
        EXTRACT(days FROM (CURRENT_DATE - MAX(d.full_date))) as days_since_last_purchase
    FROM dim_customer c
    LEFT JOIN fact_sales s ON c.customer_id = s.customer_id
    LEFT JOIN dim_date d ON s.date_id = d.date_id
    GROUP BY c.customer_id, c.first_name, c.last_name
)
SELECT 
    *,
    CASE 
        WHEN purchase_count >= 12 AND total_spend >= 1000 THEN 'VIP'
        WHEN purchase_count >= 6 AND total_spend >= 500 THEN 'Regular'
        WHEN days_since_last_purchase <= 90 THEN 'Active'
        WHEN days_since_last_purchase > 90 AND days_since_last_purchase <= 180 THEN 'At Risk'
        ELSE 'Churned'
    END as customer_segment
FROM customer_metrics;


