/*
===============================================================================
DDL Script: Create Silver Tables
===============================================================================
Script Purpose:
    This script creates tables in the 'silver' schema for the Blinkit
    data warehouse.

    Existing tables are dropped and recreated to ensure a clean DDL structure.

EXECUTION INSTRUCTIONS (PostgreSQL / pgAdmin):
    ▶ You CAN execute this entire script at once.
    ▶ Open Query Tool in pgAdmin.
    ▶ Paste the full script and click Execute (F5).
    ▶ The script will run sequentially from top to bottom.

PREREQUISITES:
    ✔ The database (e.g., data_warehouse) must exist
    ✔ The 'silver' schema must exist before execution
      (use: CREATE SCHEMA IF NOT EXISTS silver;)

NOTES:
    • Tables are created without PRIMARY KEY or FOREIGN KEY constraints
    • Data is stored as-is (raw but cleaned layer)
    • Designed for analytics and downstream Gold-layer modeling
===============================================================================
*/

-------------------------------------------------------------
--   1: blinkit_customer_info
-------------------------------------------------------------
DROP TABLE IF EXISTS silver.blinkit_customer_info;

CREATE TABLE silver.blinkit_customer_info (
    customer_id         BIGINT,
    customer_name       VARCHAR(100),
    email               VARCHAR(150),
    phone_no            VARCHAR(20),
    address             TEXT,
    area                VARCHAR(100),
    pincode             VARCHAR(10),
    registration_date   DATE,
    customer_segment    VARCHAR(50),
    total_orders        INT,
    avg_order_value     NUMERIC(10,2)
);

-------------------------------------------------------------
--   2: blinkit_inventory
-------------------------------------------------------------
DROP TABLE IF EXISTS silver.blinkit_inventory;

CREATE TABLE silver.blinkit_inventory (
    product_id        BIGINT,
    received_date     DATE,
    stock_received    INT,
    damaged_stock     INT
);

-------------------------------------------------------------
--   3: blinkit_delivery_performance
-------------------------------------------------------------
DROP TABLE IF EXISTS silver.blinkit_delivery_performance;

CREATE TABLE silver.blinkit_delivery_performance (
    order_id              BIGINT,
    delivery_partner_id   BIGINT,
    promised_time         TIMESTAMP,
    actual_time           TIMESTAMP,
    delivery_time_minutes NUMERIC(6,2),
    distance_km           NUMERIC(6,2),
    delivery_status       VARCHAR(50),
    reasons_if_delayed    TEXT
);

-------------------------------------------------------------
--   4: blinkit_customer_feedback
-------------------------------------------------------------
DROP TABLE IF EXISTS silver.blinkit_customer_feedback;

CREATE TABLE silver.blinkit_customer_feedback (
    feedback_id       BIGINT,
    order_id          BIGINT,
    customer_id       BIGINT,
    rating            INT,
    feedback_text     TEXT,
    feedback_category VARCHAR(50),
    sentiment         VARCHAR(50),
    feedback_date     DATE
);

-------------------------------------------------------------
--   5: blinkit_marketing_performance
-------------------------------------------------------------
DROP TABLE IF EXISTS silver.blinkit_marketing_performance;

CREATE TABLE silver.blinkit_marketing_performance (
    campaign_id        BIGINT,
    campaign_name      VARCHAR(150),
    campaign_date      DATE,
    target_audience    VARCHAR(100),
    channel            VARCHAR(50),
    impressions        INT,
    clicks             INT,
    conversions        INT,
    spend              NUMERIC(12,2),
    revenue_generated  NUMERIC(12,2),
    roas               NUMERIC(6,2)
);

-------------------------------------------------------------
--   6: blinkit_order_items
-------------------------------------------------------------
DROP TABLE IF EXISTS silver.blinkit_order_items;

CREATE TABLE silver.blinkit_order_items (
    order_id     BIGINT,
    product_id   BIGINT,
    quantity     INT,
    unit_price   NUMERIC(10,2),
	total_price  NUMERIC(14,2)

);

-------------------------------------------------------------
--   7: blinkit_orders
-------------------------------------------------------------
DROP TABLE IF EXISTS silver.blinkit_orders;

CREATE TABLE silver.blinkit_orders (
    order_id                BIGINT,
    customer_id             BIGINT,
    delivery_status         VARCHAR(50),
	delivery_partner_id     BIGINT,
    store_id                BIGINT,
    order_date              DATE,
    order_time              TIME,
    order_total             NUMERIC(12,2),
    payment_method          VARCHAR(50)
    
);

-------------------------------------------------------------
--   8: blinkit_products
-------------------------------------------------------------
DROP TABLE IF EXISTS silver.blinkit_products;

CREATE TABLE silver.blinkit_products (
    product_id          BIGINT,
    product_name        VARCHAR(150),
    category            VARCHAR(100),
    brand               VARCHAR(100),
    price               NUMERIC(10,2),
    mrp                 NUMERIC(10,2),
    margin_percentage   NUMERIC(5,2),
    shelf_life_days     INT,
    min_stock_level     INT,
    max_stock_level     INT
);

-------------------------------------------------------------
--   9: blinkit_category_icons
-------------------------------------------------------------
DROP TABLE IF EXISTS silver.blinkit_category_icons;

CREATE TABLE silver.blinkit_category_icons (
    category   VARCHAR(100),
    img        TEXT
);

-------------------------------------------------------------
--   10: blinkit_rating_icon
-------------------------------------------------------------
DROP TABLE IF EXISTS silver.blinkit_rating_icon;

CREATE TABLE silver.blinkit_rating_icon (
    rating   INT,
    emoji    TEXT,
    star     TEXT
);
