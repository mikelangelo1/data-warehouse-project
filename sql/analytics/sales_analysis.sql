-- Sales Trend Analysis
SELECT 
    d.year,
    d.quarter,
    d.month_name,
    st.region,
    st.store_name,
    p.category,
    COUNT(DISTINCT s.sales_id) as transaction_count,
    SUM(s.quantity) as total_units_sold,
    SUM(s.total_amount) as total_revenue,
    SUM(s.discount_amount) as total_discounts,
    SUM(s.net_amount) as net_revenue,
    AVG(s.total_amount) as avg_transaction_value
FROM fact_sales s
JOIN dim_date d ON s.date_id = d.date_id
JOIN dim_store st ON s.store_id = st.store_id
JOIN dim_product p ON s.product_id = p.product_id
GROUP BY 
    d.year,
    d.quarter,
    d.month_name,
    st.region,
    st.store_name,
    p.category
ORDER BY 
    d.year,
    d.quarter,
    d.month_name;