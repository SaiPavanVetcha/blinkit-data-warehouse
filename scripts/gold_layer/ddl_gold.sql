/*
===============================================================================
Gold Layer Creation Script (Silver -> Gold)
===============================================================================
Script Purpose:
    This script creates the GOLD analytical layer using PostgreSQL VIEWS
    built on top of the SILVER layer.

    The Gold layer is designed for:
    - BI & dashboard consumption (Power BI / Tableau / Looker)
    - Star-schema–friendly analytics
    - KPI calculations and trend analysis
    - Minimal transformation overhead (logic pushed to Silver)

    Characteristics:
    - Uses ONLY views (no tables, no materialized views)
    - Always reflects latest Silver data
    - Safe to re-run (CREATE OR REPLACE)
    - Clear separation of Dimensions and Facts

Schemas:
    Source Schema : silver
    Target Schema : gold

Usage:
    Run this script once to create/update all Gold views.
===============================================================================
*/

---------------------------------------------------------
-- DIMENSION VIEWS
---------------------------------------------------------

/* =====================================================
   Dimension: Customer Info
   ===================================================== */
CREATE OR REPLACE VIEW gold.dim_customer_info  AS
SELECT
    customer_id,
    customer_name,
    email,
    phone_no,
    address,
    area ,
    pincode,
    customer_segment  
FROM silver.blinkit_customer_info;

/* =====================================================
   Dimension: Product
   ===================================================== */
CREATE OR REPLACE VIEW gold.dim_product AS
SELECT
    product_id,
    product_name,
    category,
    brand
FROM silver.blinkit_products;

/* =====================================================
   Dimension: Date 
   ===================================================== */
CREATE OR REPLACE VIEW gold.dim_date AS
SELECT DISTINCT
    d.date,
    
	EXTRACT(DAY FROM d.date)     AS day,
    EXTRACT(MONTH FROM d.date)   AS month,
    EXTRACT(YEAR FROM d.date)    AS year,
    TRIM(TO_CHAR(d.date, 'Day')) AS weekday,
	
	CASE 
    WHEN EXTRACT(DOW FROM d.date) IN (0,6) THEN TRUE
    ELSE FALSE
    END AS is_weekend

FROM (
        SELECT registration_date::DATE AS date
        FROM silver.blinkit_customer_info

        UNION
        SELECT order_date::DATE AS date
        FROM silver.blinkit_orders

        UNION
        SELECT campaign_date::DATE
        FROM silver.blinkit_marketing_performance

        UNION
        SELECT feedback_date::DATE
        FROM silver.blinkit_customer_feedback

        UNION
        SELECT received_date::DATE
        FROM silver.blinkit_inventory
     ) d
WHERE d.date IS NOT NULL;

/* =====================================================
   Dimension: Customer Feedback
   ===================================================== */
CREATE OR REPLACE VIEW gold.dim_customer_feedback AS
SELECT
    feedback_id,
    feedback_category,
    feedback_text,
    sentiment
FROM silver.blinkit_customer_feedback;

/* =====================================================
   Dimension: Store
   ===================================================== */
CREATE OR REPLACE VIEW gold.dim_store AS
SELECT DISTINCT
    store_id
FROM silver.blinkit_orders;

/* =====================================================
   Dimension: Delivery Partner
   ===================================================== */
CREATE OR REPLACE VIEW gold.dim_delivery_partner AS
SELECT DISTINCT
    delivery_partner_id
FROM silver.blinkit_orders;

/* =====================================================
   Dimension: Marketing Performance
   ===================================================== */
CREATE OR REPLACE VIEW gold.dim_marketing_performance AS
SELECT 
    campaign_id,
    campaign_name,
    target_audience,
    channel
FROM silver.blinkit_marketing_performance;

/* =====================================================
   Dimension: Orders
   ===================================================== */

CREATE OR REPLACE VIEW gold.dim_orders AS
SELECT 
    a.order_id,
    a.payment_method,
    a.delivery_status,
    b.reasons_if_delayed
FROM silver.blinkit_orders a
LEFT JOIN silver.blinkit_delivery_performance b
    ON a.order_id = b.order_id;

/* =====================================================
   Dimension: Rating Icon
   ===================================================== */
CREATE OR REPLACE VIEW gold.dim_rating_icon AS
SELECT
    rating,
    emoji,
    star
FROM silver.blinkit_rating_icon;

/* =====================================================
   Dimension: Category Icon
   ===================================================== */
CREATE OR REPLACE VIEW gold.dim_category_icon AS
SELECT
    category,
    img AS category_icon_url
FROM silver.blinkit_category_icons;

---------------------------------------------------------
-- FACT VIEWS 
---------------------------------------------------------

/* =====================================================
   Fact: Customer Metrics
   ===================================================== */

CREATE OR REPLACE VIEW gold.fact_customer_metrics AS
SELECT
    customer_id,
    COUNT(order_id)             AS total_orders,
    SUM(order_total)            AS total_revenue,
    ROUND(AVG(order_total), 2)  AS avg_order_value,
    MIN(order_date)             AS first_order_date,
    MAX(order_date)             AS last_order_date
FROM silver.blinkit_orders
GROUP BY customer_id;

/* =====================================================
   Fact: Customer Feedback
   ===================================================== */
   
CREATE OR REPLACE VIEW gold.fact_customer_feedback AS
SELECT
    feedback_id,
    order_id,
    customer_id,
    rating,
    feedback_date
FROM silver.blinkit_customer_feedback;

/* =====================================================
   Fact: Delivery Performance
   ===================================================== */
   
CREATE OR REPLACE VIEW gold.fact_delivery_performance AS
SELECT
    order_id,
    delivery_partner_id,
	promised_time ,
    actual_time,
    delivery_time_minutes,
    distance_km,
    CASE
        WHEN delivery_status <> 'On Time' THEN 1
        ELSE 0
    END AS is_delayed
FROM silver.blinkit_delivery_performance;

/* =====================================================
   Fact: Marketing Performance
   ===================================================== */
   
CREATE OR REPLACE VIEW gold.fact_marketing_performance AS
SELECT
    campaign_id,
    campaign_date,
    impressions,
    clicks,
    conversions,
    spend,
    revenue_generated,
    roas,
    CASE
        WHEN impressions > 0 THEN clicks::NUMERIC / impressions
        ELSE 0
    END AS ctr,
    CASE
        WHEN clicks > 0 THEN conversions::NUMERIC / clicks
        ELSE 0
    END AS conversion_rate
FROM silver.blinkit_marketing_performance;

/* =====================================================
   Fact: Orders
   ===================================================== */
   
CREATE OR REPLACE VIEW gold.fact_orders AS
SELECT
    order_id,
    customer_id,
    store_id,
    delivery_partner_id,
    order_date::DATE AS order_date,
	order_time::TIME AS order_time,
    order_total
FROM silver.blinkit_orders;

/* =====================================================
   Fact: Order Items
   ===================================================== */
   
CREATE OR REPLACE VIEW gold.fact_order_items AS
SELECT
    order_id,
    product_id,
    quantity,
    unit_price,
    total_price
FROM silver.blinkit_order_items;

/* =====================================================
   Fact: Inventory
   ===================================================== */
   
CREATE OR REPLACE VIEW gold.fact_inventory AS
SELECT
    product_id,
    received_date,
    stock_received,
    damaged_stock,
    (stock_received - damaged_stock) AS usable_stock
FROM silver.blinkit_inventory;

/* =====================================================
   Fact: Product 
   ===================================================== */
   
CREATE OR REPLACE VIEW gold.fact_product AS
SELECT
    product_id,
    price,
    mrp,
    margin_percentage,
    shelf_life_days,
    min_stock_level,
    max_stock_level     
FROM silver.blinkit_products;
    