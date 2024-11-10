-- Create indexes for better query performance
CREATE INDEX idx_fact_sales_date ON fact_sales(date_id);
CREATE INDEX idx_fact_sales_store ON fact_sales(store_id);
CREATE INDEX idx_fact_sales_product ON fact_sales(product_id);
CREATE INDEX idx_fact_sales_customer ON fact_sales(customer_id);
CREATE INDEX idx_fact_inventory_date ON fact_inventory(date_id);
CREATE INDEX idx_fact_inventory_store ON fact_inventory(store_id);
CREATE INDEX idx_fact_inventory_product ON fact_inventory(product_id);