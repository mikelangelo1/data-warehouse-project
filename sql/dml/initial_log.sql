-- Initial data load for dimension tables

-- Load dim_date with 5 years of dates
INSERT INTO dim_date (
    date_id,
    full_date,
    day_of_week,
    day_name,
    day_of_month,
    day_of_year,
    week_of_year,
    month_number,
    month_name,
    quarter,
    year,
    is_weekend,
    is_holiday
)
SELECT
    TO_CHAR(datum, 'YYYYMMDD')::INT AS date_id,
    datum AS full_date,
    EXTRACT(DOW FROM datum) AS day_of_week,
    TO_CHAR(datum, 'Day') AS day_name,
    EXTRACT(DAY FROM datum) AS day_of_month,
    EXTRACT(DOY FROM datum) AS day_of_year,
    EXTRACT(WEEK FROM datum) AS week_of_year,
    EXTRACT(MONTH FROM datum) AS month_number,
    TO_CHAR(datum, 'Month') AS month_name,
    EXTRACT(QUARTER FROM datum) AS quarter,
    EXTRACT(YEAR FROM datum) AS year,
    CASE WHEN EXTRACT(DOW FROM datum) IN (0, 6) THEN TRUE ELSE FALSE END AS is_weekend,
    FALSE AS is_holiday -- Update holidays manually or through a separate process
FROM (
    SELECT '2020-01-01'::DATE + SEQUENCE.DAY AS datum
    FROM GENERATE_SERIES(0, 1825) AS SEQUENCE(DAY)
) DQ;

-- Load dim_time with 24-hour intervals
INSERT INTO dim_time (
    hour_24,
    hour_12,
    minute,
    period,
    day_part
)
SELECT
    hour_24,
    CASE WHEN hour_24 > 12 THEN hour_24 - 12 ELSE hour_24 END AS hour_12,
    0 AS minute,
    CASE WHEN hour_24 < 12 THEN 'AM' ELSE 'PM' END AS period,
    CASE 
        WHEN hour_24 BETWEEN 5 AND 11 THEN 'Morning'
        WHEN hour_24 BETWEEN 12 AND 16 THEN 'Afternoon'
        WHEN hour_24 BETWEEN 17 AND 20 THEN 'Evening'
        ELSE 'Night'
    END AS day_part
FROM GENERATE_SERIES(0, 23) AS hour_24;

-- Sample store data
INSERT INTO dim_store (
    store_key,
    store_name,
    address,
    city,
    state,
    zip_code,
    region,
    store_type,
    opening_date
)
VALUES
    ('ST001', 'Downtown Mall', '123 Main St', 'New York', 'NY', '10001', 'Northeast', 'Mall', '2020-01-01'),
    ('ST002', 'Suburban Plaza', '456 Oak Ave', 'Los Angeles', 'CA', '90001', 'West', 'Plaza', '2020-01-01'),
    ('ST003', 'City Center', '789 Market St', 'Chicago', 'IL', '60601', 'Midwest', 'Street', '2020-01-01');

-- Sample promotion data
INSERT INTO dim_promotion (
    promotion_key,
    promotion_name,
    description,
    start_date,
    end_date,
    discount_type,
    discount_rate
)
VALUES
    ('PROMO001', 'New Year Sale', 'Annual new year discount event', '2024-01-01', '2024-01-15', 'Percentage', 20.00),
    ('PROMO002', 'Summer Special', 'Summer season discounts', '2024-06-01', '2024-06-30', 'Percentage', 15.00),
    ('PROMO003', 'Holiday Bundle', 'Holiday season special offers', '2024-12-01', '2024-12-31', 'Bundle', 25.00);