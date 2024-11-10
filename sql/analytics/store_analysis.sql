-- Store Performance Dashboard
WITH store_metrics AS (
    SELECT 
        st.store_id,
        st.store_name,
        st.region,
        st.store_type,
        COUNT(DISTINCT s.sales_id) as transaction_count,
        COUNT(DISTINCT s.customer_id) as unique_customers,
        SUM(s.total_amount) as total_revenue,
        SUM(s.quantity) as total_units_sold,
        SUM(s.discount_amount) as total_discounts,
        AVG(s.total_amount) as avg_transaction_value,
        SUM(i.closing_stock) as current_inventory_value
    FROM dim_store st
    LEFT JOIN fact_sales s ON st.store_id = s.store_id
    LEFT JOIN fact_inventory i ON st.store_id = i.store_id
    WHERE st.is_active = true
    GROUP BY st.store_id, st.store_name, st.region, st.store_type
)
SELECT 
    *,
    total_revenue / NULLIF(unique_customers, 0) as revenue_per_customer,
    total_discounts / NULLIF(total_revenue, 0) * 100 as discount_percentage,
    RANK() OVER (PARTITION BY region ORDER BY total_revenue DESC) as region_rank,
    RANK() OVER (PARTITION BY store_type ORDER BY total_revenue DESC) as type_rank
FROM store_metrics
ORDER BY total_revenue DESC;