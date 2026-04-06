/*
===============================================================================
Stored Procedure: Load Silver Layer (Bronze -> Silver)
===============================================================================
Script Purpose:
    This stored procedure loads cleansed and transformed data from the
    'bronze' schema into the 'silver' schema.
    
    It performs the following actions:
    - Truncates Silver tables before each load (full refresh).
    - Applies data quality checks and validations.
    - Cleans and standardizes textual fields.
    - Derives calculated columns and business metrics.
    - Enforces basic data integrity rules.
    - Logs execution duration for each table load.

Parameters:
    None.
    This stored procedure does not accept any parameters or return any values.

Usage Example:
    CALL silver.load_silver();
===============================================================================
*/

CREATE OR REPLACE PROCEDURE silver.load_silver()
LANGUAGE plpgsql
AS $$
DECLARE
    batch_start_time TIMESTAMP;
    batch_end_time   TIMESTAMP;
    start_time       TIMESTAMP;
    end_time         TIMESTAMP;
BEGIN
    batch_start_time := clock_timestamp();

    RAISE NOTICE '==========================================';
    RAISE NOTICE 'Loading Silver Layer';
    RAISE NOTICE '==========================================';

    ---------------------------------------------------------
    -- blinkit_customer_info 
    ---------------------------------------------------------
    start_time := clock_timestamp();

    TRUNCATE TABLE silver.blinkit_customer_info;

    INSERT INTO silver.blinkit_customer_info (
        customer_id,
        customer_name,
        email,
        phone_no,
        address,
        area,
        pincode,
        registration_date,
        customer_segment,
        total_orders,
        avg_order_value
    )
    SELECT
        customer_id,
        TRIM(customer_name) AS customer_name,
        TRIM(email) AS email,

        CASE
            WHEN phone !~ '^\+91[6-9][0-9]{9}$' THEN NULL
            ELSE phone
        END AS phone_no,

        CASE
            WHEN address ~ E'[\\n\\r]'
                THEN TRIM(REGEXP_REPLACE(address, E'[\\n\\r]+', ' ', 'g'))
            ELSE TRIM(address)
        END AS address,

        TRIM(area) AS area,

        CASE
            WHEN pincode ~ '^0' THEN NULL
            ELSE pincode
        END AS pincode,

        registration_date,
        TRIM(customer_segment) AS customer_segment,

        CASE
            WHEN total_orders < 0 THEN NULL
            ELSE total_orders
        END AS total_orders,

        CASE
            WHEN avg_order_value < 0 THEN NULL
            ELSE avg_order_value
        END AS avg_order_value
    FROM bronze.blinkit_customer_info;

    end_time := clock_timestamp();
    RAISE NOTICE 'blinkit_customer_info loaded in % seconds',
        EXTRACT(EPOCH FROM end_time - start_time);

    ---------------------------------------------------------
    -- blinkit_inventory
    ---------------------------------------------------------
    start_time := clock_timestamp();

    TRUNCATE TABLE silver.blinkit_inventory;

    INSERT INTO silver.blinkit_inventory (
        product_id,
        received_date,
        stock_received,
        damaged_stock
    )
    SELECT
        product_id,
        date AS received_date,

        CASE
            WHEN stock_received < 0 THEN NULL
            WHEN damaged_stock > stock_received THEN NULL
            ELSE stock_received
        END AS stock_received,

        CASE
            WHEN damaged_stock < 0 THEN NULL
            ELSE damaged_stock
        END AS damaged_stock
    FROM bronze.blinkit_inventory;

    end_time := clock_timestamp();
    RAISE NOTICE 'blinkit_inventory loaded in % seconds',
        EXTRACT(EPOCH FROM end_time - start_time);

    ---------------------------------------------------------
    -- blinkit_delivery_performance
    ---------------------------------------------------------
    start_time := clock_timestamp();

    TRUNCATE TABLE silver.blinkit_delivery_performance;

    INSERT INTO silver.blinkit_delivery_performance (
        order_id,
        delivery_partner_id,
        promised_time,
        actual_time,
        delivery_time_minutes,
        distance_km,
        delivery_status,
        reasons_if_delayed
    )
    SELECT
        order_id,
        delivery_partner_id,
        promised_time,
        actual_time,
        delivery_minutes AS delivery_time_minutes,

        /* Ensure distance is positive */
        CASE
            WHEN distance_km < 0 THEN ABS(distance_km)
            ELSE distance_km
        END AS distance_km,

            /* Normalize delivery status */
        CASE
            WHEN delivery_minutes <= 0 THEN 'On Time'
            WHEN delivery_minutes BETWEEN 1 AND 10 THEN 'Slightly Delayed'
            ELSE 'Significantly Delayed'
        END AS delivery_status,

        /* Delay reason only for delayed orders */
        CASE
            WHEN delivery_minutes > 0
                THEN NULLIF(TRIM(reasons_if_delayed), '')
            ELSE NULL
        END AS reasons_if_delayed

    FROM (
        SELECT
            *,
            EXTRACT(EPOCH FROM (actual_time - promised_time)) / 60
                AS delivery_minutes
        FROM bronze.blinkit_delivery_performance
        WHERE order_id IS NOT NULL
          AND delivery_partner_id IS NOT NULL
          AND promised_time IS NOT NULL
          AND actual_time IS NOT NULL
    ) t;


    end_time := clock_timestamp();
    RAISE NOTICE 'blinkit_delivery_performance loaded in % seconds',
        EXTRACT(EPOCH FROM end_time - start_time);

    ---------------------------------------------------------
    -- blinkit_customer_feedback
    ---------------------------------------------------------
    start_time := clock_timestamp();

    TRUNCATE TABLE silver.blinkit_customer_feedback;

    INSERT INTO silver.blinkit_customer_feedback (
        feedback_id,
        order_id,
        customer_id,
        rating,
        feedback_text,
        feedback_category,
        sentiment,
        feedback_date
    )
    SELECT DISTINCT
        feedback_id,
        order_id,
        customer_id,

        CASE
            WHEN rating BETWEEN 1 AND 5 THEN rating
            ELSE NULL
        END AS rating,

        TRIM(feedback_text) AS feedback_text,
        TRIM(feedback_category) AS feedback_category,

        CASE
            WHEN rating >= 4 THEN 'Positive'
            WHEN rating = 3 THEN 'Neutral'
            WHEN rating <= 2 THEN 'Negative'
            ELSE NULL
        END AS sentiment,

        feedback_date::DATE AS feedback_date
    FROM bronze.blinkit_customer_feedback
    WHERE feedback_id IS NOT NULL
      AND order_id IS NOT NULL
      AND customer_id IS NOT NULL
      AND rating BETWEEN 1 AND 5
      AND feedback_text IS NOT NULL
      AND TRIM(feedback_text) <> ''
      AND feedback_category IN (
            'Delivery',
            'App Experience',
            'Customer Service',
            'Product Quality'
          )
      AND feedback_date <= CURRENT_DATE;

    end_time := clock_timestamp();
    RAISE NOTICE 'blinkit_customer_feedback loaded in % seconds',
        EXTRACT(EPOCH FROM end_time - start_time);

    ---------------------------------------------------------
    -- blinkit_marketing_performance
    ---------------------------------------------------------
    start_time := clock_timestamp();

    TRUNCATE TABLE silver.blinkit_marketing_performance;

    INSERT INTO silver.blinkit_marketing_performance (
        campaign_id,
        campaign_name,
        campaign_date,
        target_audience,
        channel,
        impressions,
        clicks,
        conversions,
        spend,
        revenue_generated,
        roas
    )
    SELECT DISTINCT
        campaign_id,
        TRIM(campaign_name) AS campaign_name,
        date AS campaign_date,
        TRIM(target_audience) AS target_audience,
        TRIM(channel) AS channel,

        CASE
            WHEN impressions < 0 THEN NULL
            ELSE impressions
        END AS impressions,

        CASE
            WHEN clicks < 0 OR clicks > impressions THEN NULL
            ELSE clicks
        END AS clicks,

        CASE
            WHEN conversions < 0 OR conversions > clicks THEN NULL
            ELSE conversions
        END AS conversions,

        CASE
            WHEN spend < 0 THEN NULL
            ELSE spend
        END AS spend,

        CASE
            WHEN revenue_generated < 0 THEN NULL
            ELSE revenue_generated
        END AS revenue_generated,

        CASE
            WHEN spend > 0
                THEN ROUND(revenue_generated / spend, 2)
            ELSE NULL
        END AS roas
    FROM bronze.blinkit_marketing_performance
    WHERE campaign_id IS NOT NULL
      AND date IS NOT NULL;

    end_time := clock_timestamp();
    RAISE NOTICE 'blinkit_marketing_performance loaded in % seconds',
        EXTRACT(EPOCH FROM end_time - start_time);

    ---------------------------------------------------------
    -- blinkit_order_items
    ---------------------------------------------------------
    start_time := clock_timestamp();

    TRUNCATE TABLE silver.blinkit_order_items;

    INSERT INTO silver.blinkit_order_items (
        order_id,
        product_id,
        quantity,
        unit_price,
        total_price
    )
    SELECT DISTINCT
        order_id,
        product_id,

        CASE
            WHEN quantity <= 0 OR quantity > 100 THEN NULL
            ELSE quantity
        END AS quantity,

        CASE
            WHEN unit_price <= 0 OR unit_price > 100000 THEN NULL
            ELSE ROUND(unit_price, 2)
        END AS unit_price,

        CASE
            WHEN quantity <= 0
              OR unit_price <= 0
              OR quantity > 100
              OR unit_price > 100000
            THEN NULL
            ELSE ROUND(quantity * unit_price, 2)
        END AS total_price
    FROM bronze.blinkit_order_items
    WHERE order_id IS NOT NULL
      AND product_id IS NOT NULL;

    end_time := clock_timestamp();
    RAISE NOTICE 'blinkit_order_items loaded in % seconds',
        EXTRACT(EPOCH FROM end_time - start_time);

    ---------------------------------------------------------
    -- blinkit_orders
    ---------------------------------------------------------
    start_time := clock_timestamp();

    TRUNCATE TABLE silver.blinkit_orders;    
   
    INSERT INTO silver.blinkit_orders (
        order_id,
        customer_id,
        delivery_partner_id,
        delivery_status,
        store_id,
        order_date,
        order_time,
        order_total,
        payment_method
    )
    SELECT DISTINCT
        order_id,
        customer_id,
        delivery_partner_id,
        delivery_status,
        store_id,
        order_date::DATE,
        order_date::TIME as order_time,

        CASE
            WHEN order_total <= 0 THEN NULL
            ELSE order_total
        END AS order_total,

        TRIM(payment_method) AS payment_method
    FROM bronze.blinkit_orders
    WHERE order_id IS NOT NULL
      AND customer_id IS NOT NULL
      AND order_date IS NOT NULL
      AND order_total > 0
      AND payment_method IN ('Cash', 'Card', 'UPI', 'Wallet');

    end_time := clock_timestamp();
    RAISE NOTICE 'blinkit_orders loaded in % seconds',
        EXTRACT(EPOCH FROM end_time - start_time);

    ---------------------------------------------------------
    -- blinkit_products
    ---------------------------------------------------------
    start_time := clock_timestamp();

    TRUNCATE TABLE silver.blinkit_products;

    INSERT INTO silver.blinkit_products (
        product_id,
        product_name,
        category,
        brand,
        price,
        mrp,
        margin_percentage,
        shelf_life_days,
        min_stock_level,
        max_stock_level
    )
    SELECT DISTINCT
        product_id,
        TRIM(product_name) AS product_name,
        TRIM(category) AS category,
        TRIM(brand) AS brand,

        CASE
            WHEN price < 0 THEN NULL
            ELSE price
        END AS price,

        CASE
            WHEN mrp < 0 THEN NULL
            ELSE mrp
        END AS mrp,

        CASE
            WHEN margin_percentage < 0 OR margin_percentage > 100 THEN NULL
            ELSE margin_percentage
        END AS margin_percentage,

        CASE
            WHEN shelf_life_days <= 0 OR shelf_life_days > 365 THEN NULL
            ELSE shelf_life_days
        END AS shelf_life_days,

        CASE
            WHEN min_stock_level < 0 THEN NULL
            ELSE min_stock_level
        END AS min_stock_level,

        CASE
            WHEN max_stock_level < 0 THEN NULL
            ELSE max_stock_level
        END AS max_stock_level
    FROM bronze.blinkit_products
    WHERE product_id IS NOT NULL
      AND price <= mrp
      AND min_stock_level <= max_stock_level;

    end_time := clock_timestamp();
    RAISE NOTICE 'blinkit_products loaded in % seconds',
        EXTRACT(EPOCH FROM end_time - start_time);

    ---------------------------------------------------------
    -- blinkit_category_icons
    ---------------------------------------------------------
    start_time := clock_timestamp();

    TRUNCATE TABLE silver.blinkit_category_icons;

    INSERT INTO silver.blinkit_category_icons (
        category,
        img
    )
    SELECT DISTINCT
        TRIM(category) AS category,
        CASE
            WHEN img IS NULL OR TRIM(img) = '' THEN NULL
            WHEN img !~* '^https?://' THEN NULL
            ELSE TRIM(img)
        END AS img
    FROM bronze.blinkit_category_icons
    WHERE category IS NOT NULL
      AND TRIM(category) <> '';

    end_time := clock_timestamp();
    RAISE NOTICE 'blinkit_category_icons loaded in % seconds',
        EXTRACT(EPOCH FROM end_time - start_time);

    ---------------------------------------------------------
    -- blinkit_rating_icon
    ---------------------------------------------------------
    start_time := clock_timestamp();

    TRUNCATE TABLE silver.blinkit_rating_icon;

    INSERT INTO silver.blinkit_rating_icon (
        rating,
        emoji,
        star
    )
    SELECT DISTINCT
        rating,
        CASE
            WHEN emoji IS NULL OR TRIM(emoji) = '' THEN NULL
            ELSE TRIM(emoji)
        END AS emoji,
        CASE
            WHEN star IS NULL OR TRIM(star) = '' THEN NULL
            ELSE TRIM(star)
        END AS star
    FROM bronze.blinkit_rating_icon
    WHERE rating BETWEEN 1 AND 5;

    end_time := clock_timestamp();
    RAISE NOTICE 'blinkit_rating_icon loaded in % seconds',
        EXTRACT(EPOCH FROM end_time - start_time);

    ---------------------------------------------------------
    -- Batch completed
    ---------------------------------------------------------
    batch_end_time := clock_timestamp();

    RAISE NOTICE '==========================================';
    RAISE NOTICE 'Silver Layer Load Completed';
    RAISE NOTICE 'Total Duration: % seconds',
        EXTRACT(EPOCH FROM batch_end_time - batch_start_time);
    RAISE NOTICE '==========================================';

EXCEPTION
    WHEN OTHERS THEN
        RAISE NOTICE '==========================================';
        RAISE NOTICE 'ERROR OCCURRED DURING SILVER LOAD';
        RAISE NOTICE 'Error Message: %', SQLERRM;
        RAISE NOTICE '==========================================';
END;
$$;

-- =============================================================================
-- Execute the Procedure
-- =============================================================================

CALL silver.load_silver();
