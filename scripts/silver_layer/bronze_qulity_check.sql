/* ============================================================
   DATA QUALITY CHECKS – bronze
   ============================================================
   Purpose:
   - Identify invalid, missing, or inconsistent customer data
   - Used in Bronze → Silver validation
   ============================================================ */
   
/* ============================================================
   1. blinkit_customer_info
   ============================================================ */

/* ------------------------------------------------------------
   Customer ID – Duplicate & NULL Check
   ------------------------------------------------------------ */
SELECT *
FROM (
    SELECT
        customer_id,
        COUNT(customer_id) AS id_count
    FROM bronze.blinkit_customer_info
    GROUP BY customer_id
) AS t
WHERE id_count > 1
   OR customer_id IS NULL;

SELECT *
FROM bronze.blinkit_customer_info
WHERE customer_id <= 0;

/* ------------------------------------------------------------
   Customer Name - Numbers or special characters in name
                 - NULL Check
				 - Leading/Trailing Spaces
   ------------------------------------------------------------ */
SELECT *
FROM bronze.blinkit_customer_info
WHERE customer_name IS Null;

SELECT *
FROM bronze.blinkit_customer_info
WHERE customer_name ~ '[0-9@#\$%]';

SELECT customer_name
FROM bronze.blinkit_customer_info
WHERE customer_name <> TRIM(customer_name);

/* ------------------------------------------------------------
   Email - Invalid Format or NULL
         - Duplicate Check
   ------------------------------------------------------------ */
SELECT email
FROM bronze.blinkit_customer_info
WHERE email IS NULL
   OR email NOT SIMILAR TO '%_@_%._%';

SELECT email, COUNT(*)
FROM bronze.blinkit_customer_info
GROUP BY email
HAVING COUNT(*) > 1;

select * from bronze.blinkit_customer_info
where email <> TRIM(email)

SELECT *
FROM bronze.blinkit_customer_info
WHERE email IN (
    SELECT email
    FROM bronze.blinkit_customer_info
    WHERE email IS NOT NULL
    GROUP BY email
    HAVING COUNT(*) > 1
)
ORDER BY email;

/* ------------------------------------------------------------
   Phone Number – NULL or Invalid Characters
                  (Allows + and digits only)
				– Must Be +91 + 10 Digits
                – Valid Indian Mobile Numbers
                  (Starts with 6–9 after +91)
				– Known Fake / Repeated Numbers
                - Leading/Trailing Spaces or Internal Spaces
   ------------------------------------------------------------ */
SELECT *
FROM bronze.blinkit_customer_info
WHERE phone IS NULL
   OR phone !~ '^\+?[0-9]+$';

SELECT *
FROM bronze.blinkit_customer_info
WHERE phone !~ '^\+91[0-9]{10}$';


SELECT phone
FROM bronze.blinkit_customer_info
WHERE phone !~ '^\+91[6-9][0-9]{9}$';

SELECT *
FROM bronze.blinkit_customer_info
WHERE phone IN (
    '+910000000000',
    '+911111111111',
    '+919999999999'
);

SELECT *
FROM bronze.blinkit_customer_info
WHERE phone <> TRIM(phone)
   OR phone LIKE '% %';

/* ------------------------------------------------------------
   Address – Quality Checks
           - NULL or empty
           - Too short
      	   - Leading/trailing spaces
	  	   – Newline / Carriage Return Characters
	  	   - Address contains only numbers
	  	   - short Address Check
   ------------------------------------------------------------ */
SELECT *
FROM bronze.blinkit_customer_info
WHERE address IS NULL
   OR TRIM(address) = ''
   OR LENGTH(TRIM(address)) < 10
   OR address <> TRIM(address);


SELECT address
FROM bronze.blinkit_customer_info
WHERE address ~ E'[\\n\\r]';

SELECT *
FROM bronze.blinkit_customer_info
WHERE address ~ '^[0-9 ]+$';

SELECT *
FROM bronze.blinkit_customer_info
WHERE LENGTH(TRIM(area)) < 3;

/* ------------------------------------------------------------
   Area – NULL or Leading/Trailing Spaces
   ------------------------------------------------------------ */
SELECT area
FROM bronze.blinkit_customer_info
WHERE area IS NULL
   OR area <> TRIM(area);
   
/* ------------------------------------------------------------
   Pincode – NULL or Leading/Trailing Spaces
   		   - Non-Numeric
		   - Starts with 0
   ------------------------------------------------------------ */
SELECT pincode
FROM bronze.blinkit_customer_info
WHERE pincode IS NULL
   OR pincode <> TRIM(pincode);

SELECT pincode
FROM bronze.blinkit_customer_info
WHERE pincode !~ '^[0-9]{6}$';

SELECT pincode
FROM bronze.blinkit_customer_info
WHERE pincode ~ '^0';

SELECT pincode
FROM bronze.blinkit_customer_info
WHERE LENGTH(pincode) <> 6;

/* ------------------------------------------------------------
   Registration Date – NULL Values
                     - Future dates
					 - Old dates
   ------------------------------------------------------------ */
SELECT *
FROM bronze.blinkit_customer_info
WHERE registration_date IS NULL;

SELECT *
FROM bronze.blinkit_customer_info
WHERE registration_date > CURRENT_DATE;

SELECT *
FROM bronze.blinkit_customer_info
WHERE registration_date < DATE '2000-01-01';

/* ------------------------------------------------------------
   Customer Segment – NULL or Leading/Trailing Spaces
   ------------------------------------------------------------ */

SELECT customer_segment
FROM bronze.blinkit_customer_info
WHERE customer_segment IS NULL
   OR customer_segment <> TRIM(customer_segment);

select distinct(customer_segment) 
from bronze.blinkit_customer_info;

/* ------------------------------------------------------------
   Total Orders – Zero, Negative, or NULL Check
   ------------------------------------------------------------ */
SELECT *
FROM bronze.blinkit_customer_info
WHERE total_orders <= 0
   OR total_orders IS NULL;


/* ------------------------------------------------------------
   Average Order Value – Zero, Negative, or NULL Check
   ------------------------------------------------------------ */
SELECT *
FROM bronze.blinkit_customer_info
WHERE avg_order_value <= 0
   OR avg_order_value IS NULL;

/* ============================================================
   2. blinkit_inventory
   ============================================================ */

/* ------------------------------------------------------------
   Product ID – NULL Check
              - Duplicate product_id per date
   ------------------------------------------------------------ */

SELECT *
FROM bronze.blinkit_inventory
WHERE product_id IS NULL;

SELECT
    product_id,
    date,
    COUNT(*) AS cnt
FROM bronze.blinkit_inventory
GROUP BY product_id, date
HAVING COUNT(*) > 1;

/* ------------------------------------------------------------
   Date – NULL Check
        - Future dates
		- Old dates
   ------------------------------------------------------------ */

SELECT *
FROM bronze.blinkit_inventory
WHERE date IS NULL;

SELECT *
FROM bronze.blinkit_inventory
WHERE date > CURRENT_DATE;

SELECT *
FROM bronze.blinkit_inventory
WHERE date < DATE '2000-01-01';

/* ------------------------------------------------------------
   Stock Received – NULL or Negative values
                  - Unrealistically high stock
   ------------------------------------------------------------ */

SELECT *
FROM bronze.blinkit_inventory
WHERE stock_received IS NULL
   OR stock_received < 0;

SELECT *
FROM bronze.blinkit_inventory
WHERE stock_received > 100000;

/* ------------------------------------------------------------
   Damaged Stock – NULL or Negative values
                 - Damaged stock greater than stock received
   ------------------------------------------------------------ */

SELECT *
FROM bronze.blinkit_inventory
WHERE damaged_stock IS NULL
   OR damaged_stock < 0;

SELECT *
FROM bronze.blinkit_inventory
WHERE damaged_stock > stock_received;

/* ============================================================
   3. blinkit_delivery_performance
   ============================================================ */

/* ------------------------------------------------------------
   Order ID – NULL Check
            - Duplicate order_id
   ------------------------------------------------------------ */

SELECT *
FROM bronze.blinkit_delivery_performance
WHERE order_id IS NULL;

SELECT
    order_id,
    COUNT(*) AS cnt
FROM bronze.blinkit_delivery_performance
GROUP BY order_id
HAVING COUNT(*) > 1;


/* ------------------------------------------------------------
   Delivery Partner ID – Duplicate & NULL Check
   ------------------------------------------------------------ */

SELECT
    delivery_partner_id,
    COUNT(*) AS cnt
FROM bronze.blinkit_delivery_performance
GROUP BY delivery_partner_id
HAVING COUNT(*) > 1;

SELECT *
FROM bronze.blinkit_delivery_performance
WHERE delivery_partner_id IS NULL;


/* ------------------------------------------------------------
   Promised Time & Actual Time – NULL Check
                               - Future timestamps
   ------------------------------------------------------------ */

SELECT *
FROM bronze.blinkit_delivery_performance
WHERE promised_time IS NULL
   OR actual_time IS NULL;

SELECT *
FROM bronze.blinkit_delivery_performance
WHERE promised_time > CURRENT_TIMESTAMP
   OR actual_time > CURRENT_TIMESTAMP;


/* ------------------------------------------------------------
   Time Logic – Promised vs Actual
              - Early delivery tracking
   ------------------------------------------------------------ */

-- Actual delivery before promised time (early delivery)
SELECT *
FROM bronze.blinkit_delivery_performance
WHERE actual_time < promised_time;


/* ------------------------------------------------------------
   Delivery Time Minutes – Sanity Checks
   ------------------------------------------------------------ */

-- NULL or extreme values
SELECT *
FROM bronze.blinkit_delivery_performance
WHERE delivery_time_minutes IS NULL
   OR delivery_time_minutes < -60
   OR delivery_time_minutes > 180;


/* ------------------------------------------------------------
   Delivery Time Minutes – Timestamp Consistency
   ------------------------------------------------------------ */

-- Mismatch between timestamps and delivery_time_minutes
SELECT *
FROM bronze.blinkit_delivery_performance
WHERE delivery_time_minutes <>
      EXTRACT(EPOCH FROM (actual_time - promised_time)) / 60;


/* ------------------------------------------------------------
   Distance (KM) – NULL, Negative, Unrealistic Values
   ------------------------------------------------------------ */

-- NULL or negative distance
SELECT *
FROM bronze.blinkit_delivery_performance
WHERE distance_km IS NULL
   OR distance_km <= 0;

-- Unrealistically large distance
SELECT *
FROM bronze.blinkit_delivery_performance
WHERE distance_km > 50;


/* ------------------------------------------------------------
   Delivery Status – Domain Validation
   ------------------------------------------------------------ */

SELECT DISTINCT delivery_status
FROM bronze.blinkit_delivery_performance
WHERE delivery_status NOT IN (
    'On Time',
    'Slightly Delayed',
    'Significantly Delayed'
);


/* ------------------------------------------------------------
   Delivery Status vs Delivery Time Logic
   ------------------------------------------------------------ */

-- On Time but delay exists
SELECT *
FROM bronze.blinkit_delivery_performance
WHERE delivery_status = 'On Time'
  AND delivery_time_minutes > 0;

-- Slightly Delayed but delay out of range (1–10 mins)
SELECT *
FROM bronze.blinkit_delivery_performance
WHERE delivery_status = 'Slightly Delayed'
  AND (delivery_time_minutes <= 0 OR delivery_time_minutes > 10);

-- Significantly Delayed but delay too small
SELECT *
FROM bronze.blinkit_delivery_performance
WHERE delivery_status = 'Significantly Delayed'
  AND delivery_time_minutes <= 10;


/* ------------------------------------------------------------
   Reasons If Delayed – Conditional Validation
   ------------------------------------------------------------ */

-- Delayed delivery without reason
SELECT *
FROM bronze.blinkit_delivery_performance
WHERE delivery_status IN ('Slightly Delayed', 'Significantly Delayed')
  AND (reasons_if_delayed IS NULL OR TRIM(reasons_if_delayed) = '');

-- On-time delivery with delay reason populated
SELECT *
FROM bronze.blinkit_delivery_performance
WHERE delivery_status = 'On Time'
  AND reasons_if_delayed IS NOT NULL;

/* ============================================================
   4. blinkit_customer_feedback
   ============================================================ */

/* ------------------------------------------------------------
   Feedback ID – NULL & Duplicate Check
   ------------------------------------------------------------ */

-- NULL feedback_id
SELECT *
FROM bronze.blinkit_customer_feedback
WHERE feedback_id IS NULL;

-- Duplicate feedback_id
SELECT
    feedback_id,
    COUNT(*) AS cnt
FROM bronze.blinkit_customer_feedback
GROUP BY feedback_id
HAVING COUNT(*) > 1;


/* ------------------------------------------------------------
   Order ID & Customer ID – NULL Check
   ------------------------------------------------------------ */

SELECT *
FROM bronze.blinkit_customer_feedback
WHERE order_id IS NULL
   OR customer_id IS NULL;


/* ------------------------------------------------------------
   Rating – NULL & Domain Validation (1–5)
   ------------------------------------------------------------ */

-- NULL ratings
SELECT *
FROM bronze.blinkit_customer_feedback
WHERE rating IS NULL;

-- Ratings outside valid range
SELECT *
FROM bronze.blinkit_customer_feedback
WHERE rating NOT BETWEEN 1 AND 5;


/* ------------------------------------------------------------
   Feedback Text – NULL & Blank Check
   ------------------------------------------------------------ */

-- NULL feedback text
SELECT *
FROM bronze.blinkit_customer_feedback
WHERE feedback_text IS NULL;

-- Blank or whitespace-only feedback
SELECT *
FROM bronze.blinkit_customer_feedback
WHERE TRIM(feedback_text) = '';

SELECT * 
FROM bronze.blinkit_customer_feedback
WHERE feedback_text ~ E'[\\n\\r]';

/* ------------------------------------------------------------
   Feedback Category – Domain Validation
   ------------------------------------------------------------ */

SELECT DISTINCT feedback_category
FROM bronze.blinkit_customer_feedback
WHERE feedback_category NOT IN (
    'Delivery',
    'App Experience',
    'Customer Service',
    'Product Quality'
);


/* ------------------------------------------------------------
   Sentiment – Domain Validation
   ------------------------------------------------------------ */

SELECT DISTINCT sentiment
FROM bronze.blinkit_customer_feedback
WHERE sentiment NOT IN (
    'Positive',
    'Neutral',
    'Negative'
);


/* ------------------------------------------------------------
   Rating vs Sentiment Consistency
   ------------------------------------------------------------ */

-- Positive sentiment with low rating
SELECT *
FROM bronze.blinkit_customer_feedback
WHERE sentiment = 'Positive'
  AND rating <= 2;

-- Negative sentiment with high rating
SELECT *
FROM bronze.blinkit_customer_feedback
WHERE sentiment = 'Negative'
  AND rating >= 4;


/* ------------------------------------------------------------
   Feedback Date – NULL & Future Date Check
   ------------------------------------------------------------ */

-- NULL feedback date
SELECT *
FROM bronze.blinkit_customer_feedback
WHERE feedback_date IS NULL;

-- Feedback date in the future
SELECT *
FROM bronze.blinkit_customer_feedback
WHERE feedback_date > CURRENT_DATE;


/* ------------------------------------------------------------
   Duplicate Record Check
   ------------------------------------------------------------ */

SELECT
    order_id,
    customer_id,
    feedback_date,
    COUNT(*) AS cnt
FROM bronze.blinkit_customer_feedback
GROUP BY
    order_id,
    customer_id,
    feedback_date
HAVING COUNT(*) > 1;


/* ------------------------------------------------------------
   Whitespace Checks
   ------------------------------------------------------------ */

SELECT *
FROM bronze.blinkit_customer_feedback
WHERE feedback_text <> TRIM(feedback_text)
   OR feedback_category <> TRIM(feedback_category)
   OR sentiment <> TRIM(sentiment);

/* ============================================================
   5. blinkit_marketing_performance 
   ============================================================ */

/* ------------------------------------------------------------
   Campaign ID – NULL & Duplicate Check
   ------------------------------------------------------------ */

-- NULL campaign_id
SELECT *
FROM bronze.blinkit_marketing_performance
WHERE campaign_id IS NULL;

-- Duplicate campaign_id per date
SELECT
    campaign_id,
    date,
    COUNT(*) AS cnt
FROM bronze.blinkit_marketing_performance
GROUP BY campaign_id, date
HAVING COUNT(*) > 1;


/* ------------------------------------------------------------
   Campaign Name – NULL & Blank Check
   ------------------------------------------------------------ */

SELECT *
FROM bronze.blinkit_marketing_performance
WHERE campaign_name IS NULL
   OR TRIM(campaign_name) = '';


/* ------------------------------------------------------------
   Campaign Date – NULL, Future Date Check
   ------------------------------------------------------------ */

-- NULL campaign date
SELECT *
FROM bronze.blinkit_marketing_performance
WHERE date IS NULL;

-- Future campaign dates
SELECT *
FROM bronze.blinkit_marketing_performance
WHERE date > CURRENT_DATE;


/* ------------------------------------------------------------
   Target Audience – NULL & Domain Validation
   ------------------------------------------------------------ */

-- NULL target audience
SELECT *
FROM bronze.blinkit_marketing_performance
WHERE target_audience IS NULL
   OR TRIM(target_audience) = '';


/* ------------------------------------------------------------
   Channel – NULL & Domain Validation
   ------------------------------------------------------------ */

-- NULL channel
SELECT *
FROM bronze.blinkit_marketing_performance
WHERE channel IS NULL
   OR TRIM(channel) = '';

-- Unexpected channel values
SELECT DISTINCT channel
FROM bronze.blinkit_marketing_performance
WHERE channel NOT IN (
    'App',
    'Email',
    'SMS',
    'Social Media'
);


/* ------------------------------------------------------------
   Impressions, Clicks, Conversions – NULL & Negative Check
   ------------------------------------------------------------ */

SELECT *
FROM bronze.blinkit_marketing_performance
WHERE impressions IS NULL OR impressions < 0
   OR clicks IS NULL OR clicks < 0
   OR conversions IS NULL OR conversions < 0;


/* ------------------------------------------------------------
   Clicks & Conversions Logic
   ------------------------------------------------------------ */

-- Clicks greater than impressions
SELECT *
FROM bronze.blinkit_marketing_performance
WHERE clicks > impressions;

-- Conversions greater than clicks
SELECT *
FROM bronze.blinkit_marketing_performance
WHERE conversions > clicks;


/* ------------------------------------------------------------
   Spend & Revenue – NULL & Negative Check
   ------------------------------------------------------------ */

SELECT *
FROM bronze.blinkit_marketing_performance
WHERE spend IS NULL OR spend < 0
   OR revenue_generated IS NULL OR revenue_generated < 0;


/* ------------------------------------------------------------
   ROAS – NULL, Negative & Consistency Check
   ------------------------------------------------------------ */

-- NULL or negative ROAS
SELECT *
FROM bronze.blinkit_marketing_performance
WHERE roas IS NULL OR roas < 0;

-- ROAS mismatch with spend & revenue
SELECT *
FROM bronze.blinkit_marketing_performance
WHERE spend > 0
  AND ROUND(revenue_generated / spend, 2) <> roas;


/* ------------------------------------------------------------
   Duplicate Record Check (All Columns)
   ------------------------------------------------------------ */

SELECT
    campaign_id,
    campaign_name,
    date,
    target_audience,
    channel,
    COUNT(*) AS cnt
FROM bronze.blinkit_marketing_performance
GROUP BY
    campaign_id,
    campaign_name,
    date,
    target_audience,
    channel
HAVING COUNT(*) > 1;

/* ============================================================
   6. blinkit_order_items
   ============================================================ */

/* ------------------------------------------------------------
   Order ID – NULL & Duplicate Check
   ------------------------------------------------------------ */

-- NULL order_id
SELECT *
FROM bronze.blinkit_order_items
WHERE order_id IS NULL;

-- Duplicate order_id + product_id (business key)
SELECT
    order_id,
    product_id,
    COUNT(*) AS cnt
FROM bronze.blinkit_order_items
GROUP BY order_id, product_id
HAVING COUNT(*) > 1;


/* ------------------------------------------------------------
   Product ID – NULL Check
   ------------------------------------------------------------ */

-- NULL product_id
SELECT *
FROM bronze.blinkit_order_items
WHERE product_id IS NULL;


/* ------------------------------------------------------------
   Quantity – NULL, Zero, Negative, Unrealistic Values
   ------------------------------------------------------------ */

-- NULL quantity
SELECT *
FROM bronze.blinkit_order_items
WHERE quantity IS NULL;

-- Zero or negative quantity
SELECT *
FROM bronze.blinkit_order_items
WHERE quantity <= 0;

-- Unrealistically large quantity
SELECT *
FROM bronze.blinkit_order_items
WHERE quantity > 100;


/* ------------------------------------------------------------
   Unit Price – NULL, Zero, Negative, Unrealistic Values
   ------------------------------------------------------------ */

-- NULL unit_price
SELECT *
FROM bronze.blinkit_order_items
WHERE unit_price IS NULL;

-- Zero or negative unit_price
SELECT *
FROM bronze.blinkit_order_items
WHERE unit_price <= 0;

-- Unrealistically high unit_price
SELECT *
FROM bronze.blinkit_order_items
WHERE unit_price > 100000;


/* ------------------------------------------------------------
   Monetary Precision Check
   ------------------------------------------------------------ */

-- Unit price with more than 2 decimal places
SELECT *
FROM bronze.blinkit_order_items
WHERE unit_price <> ROUND(unit_price, 2);


/* ------------------------------------------------------------
   Derived Value Sanity Check (quantity * unit_price)
   ------------------------------------------------------------ */

-- Extremely large order total value
SELECT *,
       quantity * unit_price AS total_price
FROM bronze.blinkit_order_items
WHERE quantity * unit_price > 1000000;

/* ============================================================
   7. blinkit_orders
   ============================================================ */

/* ------------------------------------------------------------
   Order ID – NULL & Duplicate Check
   ------------------------------------------------------------ */

-- NULL order_id
SELECT *
FROM bronze.blinkit_orders
WHERE order_id IS NULL;

-- Duplicate order_id
SELECT
    order_id,
    COUNT(*) AS cnt
FROM bronze.blinkit_orders
GROUP BY order_id
HAVING COUNT(*) > 1;


/* ------------------------------------------------------------
   Customer ID – NULL Check
   ------------------------------------------------------------ */

SELECT *
FROM bronze.blinkit_orders
WHERE customer_id IS NULL;


/* ------------------------------------------------------------
   Order Date – NULL & Future Date Check
   ------------------------------------------------------------ */

-- NULL order_date
SELECT *
FROM bronze.blinkit_orders
WHERE order_date IS NULL;

-- Order date in the future
SELECT *
FROM bronze.blinkit_orders
WHERE order_date > CURRENT_TIMESTAMP;


/* ------------------------------------------------------------
   Promised & Actual Delivery Time – NULL & Future Check
   ------------------------------------------------------------ */

-- NULL timestamps
SELECT *
FROM bronze.blinkit_orders
WHERE promised_delivery_time IS NULL
   OR actual_delivery_time IS NULL;

-- Future timestamps
SELECT *
FROM bronze.blinkit_orders
WHERE promised_delivery_time > CURRENT_TIMESTAMP
   OR actual_delivery_time > CURRENT_TIMESTAMP;


/* ------------------------------------------------------------
   Delivery Time Logic – Actual vs Promised
   ------------------------------------------------------------ */

-- Early deliveries
SELECT *
FROM bronze.blinkit_orders
WHERE actual_delivery_time < promised_delivery_time;


/* ------------------------------------------------------------
   Delivery Status – Domain Validation
   ------------------------------------------------------------ */

SELECT DISTINCT delivery_status
FROM bronze.blinkit_orders
WHERE delivery_status NOT IN (
    'On Time',
    'Slightly Delayed',
    'Significantly Delayed'
);


/* ------------------------------------------------------------
   Delivery Status vs Time Logic
   ------------------------------------------------------------ */

-- On Time but delayed
SELECT *
FROM bronze.blinkit_orders
WHERE delivery_status = 'On Time'
  AND actual_delivery_time > promised_delivery_time;

-- Slightly Delayed but delay not in 1–10 mins range
SELECT *
FROM bronze.blinkit_orders
WHERE delivery_status = 'Slightly Delayed'
  AND (
        EXTRACT(EPOCH FROM (actual_delivery_time - promised_delivery_time)) / 60 <= 0
     OR EXTRACT(EPOCH FROM (actual_delivery_time - promised_delivery_time)) / 60 > 10
  );

-- Significantly Delayed but delay ≤ 10 mins
SELECT *
FROM bronze.blinkit_orders
WHERE delivery_status = 'Significantly Delayed'
  AND EXTRACT(EPOCH FROM (actual_delivery_time - promised_delivery_time)) / 60 <= 10;


/* ------------------------------------------------------------
   Order Total – NULL, Zero & Negative Check
   ------------------------------------------------------------ */

SELECT *
FROM bronze.blinkit_orders
WHERE order_total IS NULL
   OR order_total <= 0;


/* ------------------------------------------------------------
   Payment Method – Domain Validation
   ------------------------------------------------------------ */

SELECT DISTINCT payment_method
FROM bronze.blinkit_orders
WHERE payment_method NOT IN (
    'Cash',
    'Card',
    'UPI',
    'Wallet'
);


/* ------------------------------------------------------------
   Delivery Partner ID & Store ID – NULL Check
   ------------------------------------------------------------ */

SELECT *
FROM bronze.blinkit_orders
WHERE delivery_partner_id IS NULL
   OR store_id IS NULL;


/* ------------------------------------------------------------
   Duplicate Full Record Check
   ------------------------------------------------------------ */

SELECT
    order_id,
    customer_id,
    order_date,
    promised_delivery_time,
    actual_delivery_time,
    delivery_status,
    order_total,
    payment_method,
    delivery_partner_id,
    store_id,
    COUNT(*) AS cnt
FROM bronze.blinkit_orders
GROUP BY
    order_id,
    customer_id,
    order_date,
    promised_delivery_time,
    actual_delivery_time,
    delivery_status,
    order_total,
    payment_method,
    delivery_partner_id,
    store_id
HAVING COUNT(*) > 1;
/* ============================================================
   8. blinkit_products
   ============================================================ */

/* ------------------------------------------------------------
   Product ID – NULL & Duplicate Check
   ------------------------------------------------------------ */

-- NULL product_id
SELECT *
FROM bronze.blinkit_products
WHERE product_id IS NULL;

-- Duplicate product_id
SELECT
    product_id,
    COUNT(*) AS cnt
FROM bronze.blinkit_products
GROUP BY product_id
HAVING COUNT(*) > 1;


/* ------------------------------------------------------------
   Product Name – NULL, Blank & Whitespace Check
   ------------------------------------------------------------ */

-- NULL product_name
SELECT *
FROM bronze.blinkit_products
WHERE product_name IS NULL;

-- Blank or whitespace-only product_name
SELECT *
FROM bronze.blinkit_products
WHERE TRIM(product_name) = '';

-- Leading/trailing spaces
SELECT *
FROM bronze.blinkit_products
WHERE product_name <> TRIM(product_name);


/* ------------------------------------------------------------
   Category – NULL & Blank Check
   ------------------------------------------------------------ */

SELECT *
FROM bronze.blinkit_products
WHERE category IS NULL
   OR TRIM(category) = '';


/* ------------------------------------------------------------
   Brand – NULL & Blank Check
   ------------------------------------------------------------ */

SELECT *
FROM bronze.blinkit_products
WHERE brand IS NULL
   OR TRIM(brand) = '';


/* ------------------------------------------------------------
   Price & MRP – NULL, Negative & Logical Checks
   ------------------------------------------------------------ */

-- NULL or negative price
SELECT *
FROM bronze.blinkit_products
WHERE price IS NULL
   OR price < 0;

-- NULL or negative MRP
SELECT *
FROM bronze.blinkit_products
WHERE mrp IS NULL
   OR mrp < 0;

-- Price greater than MRP
SELECT *
FROM bronze.blinkit_products
WHERE price > mrp;


/* ------------------------------------------------------------
   Margin Percentage – NULL & Range Validation
   ------------------------------------------------------------ */

SELECT *
FROM bronze.blinkit_products
WHERE margin_percentage IS NULL
   OR margin_percentage < 0
   OR margin_percentage > 100;


/* ------------------------------------------------------------
   Shelf Life (Days) – NULL & Sanity Checks
   ------------------------------------------------------------ */

SELECT *
FROM bronze.blinkit_products
WHERE shelf_life_days IS NULL
   OR shelf_life_days <= 0
   OR shelf_life_days > 365;


/* ------------------------------------------------------------
   Stock Levels – NULL, Negative & Logical Checks
   ------------------------------------------------------------ */

-- NULL or negative min stock
SELECT *
FROM bronze.blinkit_products
WHERE min_stock_level IS NULL
   OR min_stock_level < 0;

-- NULL or negative max stock
SELECT *
FROM bronze.blinkit_products
WHERE max_stock_level IS NULL
   OR max_stock_level < 0;

-- Min stock greater than max stock
SELECT *
FROM bronze.blinkit_products
WHERE min_stock_level > max_stock_level;


/* ------------------------------------------------------------
   Duplicate Records – Full Row Check
   ------------------------------------------------------------ */

SELECT
    product_id,
    product_name,
    category,
    brand,
    price,
    mrp,
    margin_percentage,
    shelf_life_days,
    min_stock_level,
    max_stock_level,
    COUNT(*) AS cnt
FROM bronze.blinkit_products
GROUP BY
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
HAVING COUNT(*) > 1;

/* ============================================================
   9. blinkit_category_image
   ============================================================ */

/* ------------------------------------------------------------
   Category – NULL, Blank & Duplicate Check
   ------------------------------------------------------------ */

-- NULL category
SELECT *
FROM bronze.blinkit_category_image
WHERE category IS NULL;

-- Blank or whitespace-only category
SELECT *
FROM bronze.blinkit_category_image
WHERE TRIM(category) = '';

-- Duplicate category names
SELECT
    category,
    COUNT(*) AS cnt
FROM bronze.blinkit_category_image
GROUP BY category
HAVING COUNT(*) > 1;


/* ------------------------------------------------------------
   Image URL – NULL, Blank & Format Check
   ------------------------------------------------------------ */

-- NULL image URL
SELECT *
FROM bronze.blinkit_category_image
WHERE img IS NULL;

-- Blank or whitespace-only image URL
SELECT *
FROM bronze.blinkit_category_image
WHERE TRIM(img) = '';

-- Invalid or malformed URLs
SELECT *
FROM bronze.blinkit_category_image
WHERE img !~* '^https?://';


/* ------------------------------------------------------------
   Category & Image Mapping Consistency
   ------------------------------------------------------------ */

-- Same category mapped to multiple images
SELECT
    category,
    COUNT(DISTINCT img) AS img_count
FROM bronze.blinkit_category_image
GROUP BY category
HAVING COUNT(DISTINCT img) > 1;

-- Same image mapped to multiple categories
SELECT
    img,
    COUNT(DISTINCT category) AS category_count
FROM bronze.blinkit_category_image
GROUP BY img
HAVING COUNT(DISTINCT category) > 1;


/* ------------------------------------------------------------
   Whitespace & Formatting Check
   ------------------------------------------------------------ */

-- Leading/trailing spaces in category
SELECT *
FROM bronze.blinkit_category_image
WHERE category <> TRIM(category);

-- Leading/trailing spaces in image URL
SELECT *
FROM bronze.blinkit_category_image
WHERE img <> TRIM(img);

/* ------------------------------------------------------------
   Special Character / Encoding Check
   ------------------------------------------------------------ */

-- Non-printable or unexpected characters in category
SELECT *
FROM bronze.blinkit_category_image
WHERE category ~ '[^[:print:]]';

/* ============================================================
   10. blinkit_rating_icon
   ============================================================ */

/* ------------------------------------------------------------
   Rating – NULL Check
           - Domain validation (1–5)
           - Duplicate rating
   ------------------------------------------------------------ */

-- NULL ratings
SELECT *
FROM bronze.blinkit_rating_icon
WHERE rating IS NULL;

-- Ratings outside valid range
SELECT *
FROM bronze.blinkit_rating_icon
WHERE rating NOT BETWEEN 1 AND 5;

-- Duplicate ratings
SELECT
    rating,
    COUNT(*) AS cnt
FROM bronze.blinkit_rating_icon
GROUP BY rating
HAVING COUNT(*) > 1;

/* ------------------------------------------------------------
   Emoji – NULL, Blank & Format Check
   ------------------------------------------------------------ */

-- NULL emoji
SELECT *
FROM bronze.blinkit_rating_icon
WHERE emoji IS NULL;

-- Blank or whitespace-only emoji
SELECT *
FROM bronze.blinkit_rating_icon
WHERE TRIM(emoji) = '';

-- Non-URL or malformed emoji values
SELECT *
FROM bronze.blinkit_rating_icon
WHERE emoji !~* '^https?://';

/* ------------------------------------------------------------
   Star – NULL, Blank & Encoding Check
   ------------------------------------------------------------ */

-- NULL star values
SELECT *
FROM bronze.blinkit_rating_icon
WHERE star IS NULL;

-- Blank or whitespace-only star values
SELECT *
FROM bronze.blinkit_rating_icon
WHERE TRIM(star) = '';

-- Star length mismatch (rating vs star count)
SELECT *
FROM bronze.blinkit_rating_icon
WHERE LENGTH(star) < rating;

/* ------------------------------------------------------------
   Rating vs Star Consistency
   ------------------------------------------------------------ */

-- Rating does not match number of stars
SELECT *
FROM bronze.blinkit_rating_icon
WHERE rating <> LENGTH(star);

/* ------------------------------------------------------------
   Duplicate Record Check (All Columns)
   ------------------------------------------------------------ */

SELECT
    rating,
    emoji,
    star,
    COUNT(*) AS cnt
FROM bronze.blinkit_rating_icon
GROUP BY
    rating,
    emoji,
    star
HAVING COUNT(*) > 1;

/* ------------------------------------------------------------
   Whitespace Check
   ------------------------------------------------------------ */

-- Leading/trailing spaces in emoji
SELECT *
FROM bronze.blinkit_rating_icon
WHERE emoji <> TRIM(emoji);

-- Leading/trailing spaces in star
SELECT *
FROM bronze.blinkit_rating_icon
WHERE star <> TRIM(star);

