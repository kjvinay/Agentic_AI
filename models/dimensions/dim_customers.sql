-- ═══════════════════════════════════════════════════════════
-- models/dim_customers.sql
-- dbt model for DIM_CUSTOMERS dimension table
-- Loads from SRC_CUSTOMER source
-- ═══════════════════════════════════════════════════════════

WITH source AS (

    SELECT * FROM {{ source('foundation', 'SRC_CUSTOMER') }}

),

order_aggregates AS (

    SELECT
        CUSTOMER_ID,
        -- [SCRUM-33] AGGREGATE: Added by schema-change agent
        COUNT(src.ORDER_ID) AS TOTAL_ORDERS

    FROM {{ source('foundation', 'SRC_ORDERS') }} src
    GROUP BY CUSTOMER_ID

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

final AS (

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
        segment,
        customer_status,
        annual_revenue,
        effective_date,
        expiry_date,
        is_current,
        TOTAL_ORDERS

    FROM segmented
    LEFT JOIN order_aggregates ON segmented.customer_id = order_aggregates.customer_id

)

SELECT * FROM final