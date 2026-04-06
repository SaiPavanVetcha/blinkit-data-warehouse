/*
===============================================================================
Stored Procedure: Load Bronze Layer (Source -> Bronze)
===============================================================================
Script Purpose:
    This stored procedure loads data into the 'bronze' schema from external CSV files. 
    It performs the following actions:
    - Truncates the bronze tables before loading data.
    - Uses the `BULK INSERT` command to load data from csv Files to bronze tables.

Parameters:
    None. 
	  This stored procedure does not accept any parameters or return any values.

Usage Example:
    CALL bronze.load_bronze_full();
===============================================================================
*/
CREATE OR REPLACE PROCEDURE bronze.load_bronze_full()
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
    RAISE NOTICE 'Loading Bronze Layer (FULL LOAD)';
    RAISE NOTICE '==========================================';

    ---------------------------------------------------------
    -- blinkit_customer_info
    ---------------------------------------------------------
    start_time := clock_timestamp();
    TRUNCATE TABLE bronze.blinkit_customer_info;

    COPY bronze.blinkit_customer_info
    FROM 'D:\Blinkit\blinkit_customer_info.csv'
    CSV HEADER;

    end_time := clock_timestamp();
    RAISE NOTICE 'blinkit_customer_info loaded in % seconds',
        EXTRACT(EPOCH FROM end_time - start_time);

    ---------------------------------------------------------
    -- blinkit_inventory
    ---------------------------------------------------------
    start_time := clock_timestamp();
    TRUNCATE TABLE bronze.blinkit_inventory;

    COPY bronze.blinkit_inventory
    FROM 'D:\Blinkit\blinkit_inventory.csv'
    CSV HEADER;

    end_time := clock_timestamp();
    RAISE NOTICE 'blinkit_inventory loaded in % seconds',
        EXTRACT(EPOCH FROM end_time - start_time);

    ---------------------------------------------------------
    -- blinkit_delivery_performance
    ---------------------------------------------------------
    start_time := clock_timestamp();
    TRUNCATE TABLE bronze.blinkit_delivery_performance;

    COPY bronze.blinkit_delivery_performance
    FROM 'D:\Blinkit\blinkit_delivery_performance.csv'
    CSV HEADER;

    end_time := clock_timestamp();
    RAISE NOTICE 'blinkit_delivery_performance loaded in % seconds',
        EXTRACT(EPOCH FROM end_time - start_time);

    ---------------------------------------------------------
    -- blinkit_customer_feedback
    ---------------------------------------------------------
    start_time := clock_timestamp();
    TRUNCATE TABLE bronze.blinkit_customer_feedback;

    COPY bronze.blinkit_customer_feedback
    FROM 'D:\Blinkit\blinkit_customer_feedback.csv'
    CSV HEADER;

    end_time := clock_timestamp();
    RAISE NOTICE 'blinkit_customer_feedback loaded in % seconds',
        EXTRACT(EPOCH FROM end_time - start_time);

    ---------------------------------------------------------
    -- blinkit_marketing_performance
    ---------------------------------------------------------
    start_time := clock_timestamp();
    TRUNCATE TABLE bronze.blinkit_marketing_performance;

    COPY bronze.blinkit_marketing_performance
    FROM 'D:\Blinkit\blinkit_marketing_performance.csv'
    CSV HEADER;

    end_time := clock_timestamp();
    RAISE NOTICE 'blinkit_marketing_performance loaded in % seconds',
        EXTRACT(EPOCH FROM end_time - start_time);

    ---------------------------------------------------------
    -- blinkit_order_items
    ---------------------------------------------------------
    start_time := clock_timestamp();
    TRUNCATE TABLE bronze.blinkit_order_items;

    COPY bronze.blinkit_order_items
    FROM 'D:\Blinkit\blinkit_order_items.csv'
    CSV HEADER;

    end_time := clock_timestamp();
    RAISE NOTICE 'blinkit_order_items loaded in % seconds',
        EXTRACT(EPOCH FROM end_time - start_time);

    ---------------------------------------------------------
    -- blinkit_orders
    ---------------------------------------------------------
    start_time := clock_timestamp();
    TRUNCATE TABLE bronze.blinkit_orders;

    COPY bronze.blinkit_orders
    FROM 'D:\Blinkit\blinkit_orders.csv'
    CSV HEADER;

    end_time := clock_timestamp();
    RAISE NOTICE 'blinkit_orders loaded in % seconds',
        EXTRACT(EPOCH FROM end_time - start_time);

    ---------------------------------------------------------
    -- blinkit_products
    ---------------------------------------------------------
    start_time := clock_timestamp();
    TRUNCATE TABLE bronze.blinkit_products;

    COPY bronze.blinkit_products
    FROM 'D:\Blinkit\blinkit_products.csv'
    CSV HEADER;

    end_time := clock_timestamp();
    RAISE NOTICE 'blinkit_products loaded in % seconds',
        EXTRACT(EPOCH FROM end_time - start_time);

    ---------------------------------------------------------
    -- blinkit_category_icons
    ---------------------------------------------------------
    start_time := clock_timestamp();
    TRUNCATE TABLE bronze.blinkit_category_icons;

    COPY bronze.blinkit_category_icons
    FROM 'D:\Blinkit\blinkit_category_icons.csv'
    CSV HEADER;

    end_time := clock_timestamp();
    RAISE NOTICE 'blinkit_category_icons loaded in % seconds',
        EXTRACT(EPOCH FROM end_time - start_time);

    ---------------------------------------------------------
    -- blinkit_rating_icon
    ---------------------------------------------------------
    start_time := clock_timestamp();
    TRUNCATE TABLE bronze.blinkit_rating_icon;

    COPY bronze.blinkit_rating_icon
    FROM 'D:\Blinkit\blinkit_rating_icon.csv'
    CSV HEADER;

    end_time := clock_timestamp();
    RAISE NOTICE 'blinkit_rating_icon loaded in % seconds',
        EXTRACT(EPOCH FROM end_time - start_time);

    ---------------------------------------------------------
    -- Batch completed
    ---------------------------------------------------------
    batch_end_time := clock_timestamp();

    RAISE NOTICE '==========================================';
    RAISE NOTICE 'Bronze Layer Load Completed';
    RAISE NOTICE 'Total Duration: % seconds',
        EXTRACT(EPOCH FROM batch_end_time - batch_start_time);
    RAISE NOTICE '==========================================';

EXCEPTION
    WHEN OTHERS THEN
        RAISE NOTICE '==========================================';
        RAISE NOTICE 'ERROR OCCURRED DURING BRONZE LOAD';
        RAISE NOTICE 'Error Message: %', SQLERRM;
        RAISE NOTICE '==========================================';
END;
$$;

-- =============================================================================
-- Execute the Procedure
-- =============================================================================

CALL bronze.load_bronze_full();

