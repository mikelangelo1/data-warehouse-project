
-- Store dimension
CREATE TABLE dim_store (
    store_id SERIAL PRIMARY KEY,
    store_key VARCHAR(50) UNIQUE NOT NULL,
    store_name VARCHAR(100) NOT NULL,
    address TEXT,
    city VARCHAR(50),
    state VARCHAR(2),
    zip_code VARCHAR(10),
    region VARCHAR(50),
    store_type VARCHAR(50),
    opening_date DATE,
    closing_date DATE,
    is_active BOOLEAN DEFAULT true,
    last_update_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Date dimension
CREATE TABLE dim_date (
    date_id INTEGER PRIMARY KEY,
    full_date DATE UNIQUE NOT NULL,
    day_of_week INTEGER,
    day_name VARCHAR(10),
    day_of_month INTEGER,
    day_of_year INTEGER,
    week_of_year INTEGER,
    month_number INTEGER,
    month_name VARCHAR(10),
    quarter INTEGER,
    year INTEGER,
    is_weekend BOOLEAN,
    is_holiday BOOLEAN
);

-- Time dimension
CREATE TABLE dim_time (
    time_id SERIAL PRIMARY KEY,
    hour_24 INTEGER,
    hour_12 INTEGER,
    minute INTEGER,
    period VARCHAR(2),
    day_part VARCHAR(20)
);

-- Promotion dimension
CREATE TABLE dim_promotion (
    promotion_id SERIAL PRIMARY KEY,
    promotion_key VARCHAR(50) UNIQUE NOT NULL,
    promotion_name VARCHAR(100),
    description TEXT,
    start_date DATE,
    end_date DATE,
    discount_type VARCHAR(50),
    discount_rate DECIMAL(5,2),
    minimum_purchase DECIMAL(10,2),
    is_active BOOLEAN DEFAULT true
);

-- Fact Tables
CREATE TABLE fact_sales (
    sales_id SERIAL PRIMARY KEY,
    transaction_key VARCHAR(50) UNIQUE NOT NULL,
    date_id INTEGER REFERENCES dim_date(date_id),
    time_id INTEGER REFERENCES dim_time(time_id),
    store_id INTEGER REFERENCES dim_store(store_id),
    product_id INTEGER REFERENCES dim_product(product_id),
    customer_id INTEGER REFERENCES dim_customer(customer_id),
    promotion_id INTEGER REFERENCES dim_promotion(promotion_id),
    quantity INTEGER NOT NULL,
    unit_price DECIMAL(10,2) NOT NULL,
    discount_amount DECIMAL(10,2),
    net_amount DECIMAL(10,2),
    tax_amount DECIMAL(10,2),
    total_amount DECIMAL(10,2),
    payment_method VARCHAR(50),
    created_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE fact_inventory (
    inventory_id SERIAL PRIMARY KEY,
    date_id INTEGER REFERENCES dim_date(date_id),
    store_id INTEGER REFERENCES dim_store(store_id),
    product_id INTEGER REFERENCES dim_product(product_id),
    opening_stock INTEGER,
    received_stock INTEGER,
    sold_stock INTEGER,
    damaged_stock INTEGER,
    closing_stock INTEGER,
    minimum_stock_level INTEGER,
    maximum_stock_level INTEGER,
    reorder_point INTEGER,
    stock_value DECIMAL(10,2),
    created_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

