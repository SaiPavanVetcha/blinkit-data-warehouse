/*
===============================================================================
DDL Script: Create Bronze Tables
===============================================================================
Script Purpose:
    This script creates tables in the 'bronze' schema for the data warehouse.

    Existing tables are dropped and recreated to ensure a clean and
    consistent Bronze-layer structure.

EXECUTION INSTRUCTIONS (PostgreSQL / pgAdmin):
    ▶ You CAN execute this entire script at once.
    ▶ Open Query Tool in pgAdmin.
    ▶ Paste the full script and click Execute (F5).
    ▶ Statements will execute sequentially from top to bottom.

PREREQUISITES:
    ✔ The target database (e.g., data_warehouse) must exist
    ✔ The 'bronze' schema must exist prior to execution
      (or use: CREATE SCHEMA IF NOT EXISTS bronze;)

NOTES:
    • Bronze tables store raw, source-aligned data
    • No PRIMARY KEY or FOREIGN KEY constraints are enforced
    • Minimal transformation logic is applied in this layer
    • Designed as the ingestion foundation for Silver processing
===============================================================================
*/

-------------------------------------------------------------
--   1: blinkit_customer_info
-------------------------------------------------------------
DROP TABLE IF EXISTS bronze.blinkit_customer_info;

CREATE TABLE bronze.blinkit_customer_info (
    customer_id         BIGINT,
    customer_name       VARCHAR(100),
    email               VARCHAR(150),
    phone               VARCHAR(20),
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
DROP TABLE IF EXISTS bronze.blinkit_inventory;

CREATE TABLE bronze.blinkit_inventory (
    product_id        BIGINT,
    date              DATE,
    stock_received    INT,
    damaged_stock     INT
);

-------------------------------------------------------------
--   3: blinkit_delivery_performance
-------------------------------------------------------------
DROP TABLE IF EXISTS bronze.blinkit_delivery_performance;

CREATE TABLE bronze.blinkit_delivery_performance (
    order_id              BIGINT,
    delivery_partner_id   BIGINT,
    promised_time         TIMESTAMP,
    actual_time           TIMESTAMP,
    delivery_time_minutes NUMERIC,
    distance_km           NUMERIC(6,2),
    delivery_status       VARCHAR(50),
    reasons_if_delayed    TEXT
);

-------------------------------------------------------------
--   4: blinkit_customer_feedback
-------------------------------------------------------------
DROP TABLE IF EXISTS bronze.blinkit_customer_feedback;

CREATE TABLE bronze.blinkit_customer_feedback (
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
DROP TABLE IF EXISTS bronze.blinkit_marketing_performance;

CREATE TABLE bronze.blinkit_marketing_performance (
    campaign_id        BIGINT,
    campaign_name      VARCHAR(150),
    date               DATE,
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
DROP TABLE IF EXISTS bronze.blinkit_order_items;

CREATE TABLE bronze.blinkit_order_items (
    order_id    BIGINT,
    product_id  BIGINT,
    quantity    INT,
    unit_price  NUMERIC(10,2)
);

-------------------------------------------------------------
--   7: blinkit_orders
-------------------------------------------------------------
DROP TABLE IF EXISTS bronze.blinkit_orders;

CREATE TABLE bronze.blinkit_orders (
    order_id                BIGINT,
    customer_id             BIGINT,
    order_date              TIMESTAMP,
    promised_delivery_time  TIMESTAMP,
    actual_delivery_time    TIMESTAMP,
    delivery_status         VARCHAR(50),
    order_total             NUMERIC(12,2),
    payment_method          VARCHAR(50),
    delivery_partner_id     BIGINT,
    store_id                BIGINT
);

-------------------------------------------------------------
--   8: blinkit_products
-------------------------------------------------------------
DROP TABLE IF EXISTS bronze.blinkit_products;

CREATE TABLE bronze.blinkit_products (
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
DROP TABLE IF EXISTS bronze.blinkit_category_icons;

CREATE TABLE bronze.blinkit_category_icons (
    category   VARCHAR(100),
    img        TEXT
);

-------------------------------------------------------------
--   10: blinkit_rating_icon
-------------------------------------------------------------
DROP TABLE IF EXISTS bronze.blinkit_rating_icon;

CREATE TABLE bronze.blinkit_rating_icon (
    rating   INT,
    emoji    TEXT,
    star     TEXT
);
