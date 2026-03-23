-- ═══════════════════════════════════════════════════════════
-- models/fact_orders.sql
-- dbt model for FACT_ORDERS denormalised fact table
-- Joins SRC_ORDERS → SRC_CUSTOMER → SRC_PRODUCTS → SRC_SALESREP
-- ═══════════════════════════════════════════════════════════

WITH orders AS (

    SELECT * FROM {{ source('foundation', 'SRC_ORDERS') }}

),

customers AS (

    SELECT * FROM {{ source('foundation', 'SRC_CUSTOMER') }}

),

products AS (

    SELECT * FROM {{ source('foundation', 'SRC_PRODUCTS') }}

),

salesrep AS (

    SELECT * FROM {{ source('foundation', 'SRC_SALESREP') }}

),

joined AS (

    SELECT
        o.ORDER_ID,
        o.CUSTOMER_ID,
        o.PRODUCT_ID,
        o.SALES_REP_ID,
        o.ORDER_DATE,
        o.SHIP_DATE,
        o.QUANTITY,
        o.UNIT_PRICE,
        o.DISCOUNT_PCT,
        o.ORDER_STATUS,
        o.REGION,
        o.NOTES,

        -- Customer fields
        c.CUSTOMER_NAME,
        c.EMAIL                                 AS customer_email,
        c.CITY                                  AS customer_city,
        c.STATE                                 AS customer_state,
        c.INDUSTRY,
        c.EMPLOYEE_COUNT                        AS customer_count,

        -- Product fields
        p.PRODUCT_NAME,
        p.PRODUCT_CODE,
        p.CATEGORY                              AS product_category,
        p.UNIT_COST,
        p.LIST_PRICE,

        -- Sales rep fields
        s.SALES_REP_NAME,
        s.REGION                                AS rep_region

    FROM orders          o
    LEFT JOIN customers  c ON o.CUSTOMER_ID  = c.CUSTOMER_ID
    LEFT JOIN products   p ON o.PRODUCT_ID   = p.PRODUCT_ID
    LEFT JOIN salesrep   s ON o.SALES_REP_ID = s.SALES_REP_ID

),

calculated AS (

    SELECT
        ORDER_ID                                AS order_key,
        ORDER_ID,
        CUSTOMER_ID,
        CUSTOMER_NAME,

        CASE
            WHEN customer_count < 250               THEN 'Small'
            WHEN customer_count BETWEEN 250 AND 500 THEN 'Mid Size'
            ELSE                                         'Enterprise'
        END                                     AS customer_segment,

        PRODUCT_NAME,
        PRODUCT_CODE,
        PRODUCT_CATEGORY,
        SALES_REP_NAME,
        REGION,
        INDUSTRY,
        ORDER_DATE,
        SHIP_DATE,
        ORDER_STATUS,

        QUANTITY,
        UNIT_PRICE,
        DISCOUNT_PCT,
        LIST_PRICE,
        UNIT_COST,

        -- Derived metrics
        ROUND(QUANTITY * UNIT_PRICE, 2)                             AS gross_amount,
        ROUND(QUANTITY * UNIT_PRICE * (1 - DISCOUNT_PCT / 100), 2) AS net_amount,
        ROUND(QUANTITY * UNIT_COST, 2)                              AS cost_amount,
        ROUND(
            QUANTITY * UNIT_PRICE * (1 - DISCOUNT_PCT / 100)
            - QUANTITY * UNIT_COST,
            2
        )                                                           AS profit_amount,

        DATEDIFF('day', ORDER_DATE, COALESCE(SHIP_DATE, CURRENT_DATE())) AS days_to_ship

    FROM joined

),

final AS (

    SELECT
        order_key,
        order_id,
        customer_id,
        customer_name,
        customer_segment,
        product_name,
        product_code,
        product_category,
        sales_rep_name,
        region,
        industry,
        order_date,
        ship_date,
        order_status,
        quantity,
        unit_price,
        discount_pct,
        gross_amount,
        net_amount,
        cost_amount,
        profit_amount,
        days_to_ship

    FROM calculated

)

SELECT * FROM final
