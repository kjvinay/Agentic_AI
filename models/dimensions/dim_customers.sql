-- ═══════════════════════════════════════════════════════════
-- models/dim_customers.sql
-- dbt model for DIM_CUSTOMERS dimension table
-- Loads from SRC_CUSTOMER source
-- ═══════════════════════════════════════════════════════════

WITH source AS (

    SELECT * FROM {{ source('foundation', 'SRC_CUSTOMER') }}

),

renamed AS (

    SELECT
        CUSTOMER_ID                             AS customer_key,
        CUSTOMER_ID,
        CUSTOMER_NAME,
        EMAIL,
        PHONE,
        ADDRESS_LINE1,
        CITY,
        STATE,
        ZIP_CODE,
        COUNTRY,
        INDUSTRY,
        EMPLOYEE_COUNT                          AS customer_count,
        ANNUAL_REVENUE,
        CREATED_AT                              AS effective_date,
        '9999-12-31'::DATE                      AS expiry_date,
        IS_ACTIVE                               AS is_current

    FROM source

),

segmented AS (

    SELECT
        customer_key,
        customer_id,
        customer_name,
        email,
        phone,
        address_line1,
        city,
        state,
        zip_code,
        country,
        industry,
        customer_count,
        annual_revenue,
        effective_date,
        expiry_date,
        is_current,

        CASE
            WHEN customer_count < 250               THEN 'Small'
            WHEN customer_count BETWEEN 250 AND 500 THEN 'Mid Size'
            ELSE                                         'Enterprise'
        END                                     AS segment,

        CASE
            WHEN is_current = TRUE              THEN 'Active'
            ELSE                                     'Inactive'
        END                                     AS customer_status

    FROM renamed

),

orders_agg AS (

    SELECT
        CUSTOMER_ID,
        -- [SCRUM-40] AGGREGATE: Added by schema-change agent
        COUNT(ORDER_ID) AS TOTAL_ORDERS

    FROM {{ source('foundation', 'SRC_ORDERS') }}
    GROUP BY CUSTOMER_ID

),

final AS (

    SELECT
        s.customer_key,
        s.customer_id,
        s.customer_name,
        s.email,
        s.phone,
        s.address_line1,
        s.city,
        s.state,
        s.zip_code,
        s.country,
        s.industry,
        s.customer_count,
        s.segment,
        s.customer_status,
        s.annual_revenue,
        s.effective_date,
        s.expiry_date,
        s.is_current,
        COALESCE(o.TOTAL_ORDERS, 0) AS TOTAL_ORDERS

    FROM segmented s
    LEFT JOIN orders_agg o ON s.customer_id = o.CUSTOMER_ID

)

SELECT * FROM final