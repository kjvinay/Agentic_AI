-- ═══════════════════════════════════════════════════════════
-- models/dim_customers.sql
-- dbt model for DIM_CUSTOMERS dimension table
-- Loads from SRC_CUSTOMER source
-- ═══════════════════════════════════════════════════════════

WITH source AS (

    SELECT * FROM {{ source('foundation', 'SRC_CUSTOMER') }}

),

orders AS (

    SELECT
        CUSTOMER_ID,
        -- [SCRUM-35] AGGREGATE: Added by schema-change agent
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
        segmented.customer_key,
        segmented.customer_id,
        segmented.customer_name,
        segmented.email,
        segmented.phone,
        segmented.address_line1,
        segmented.city,
        segmented.state,
        segmented.zip_code,
        segmented.country,
        segmented.industry,
        segmented.customer_count,
        segmented.segment,
        segmented.customer_status,
        segmented.annual_revenue,
        segmented.effective_date,
        segmented.expiry_date,
        segmented.is_current,
        orders.TOTAL_ORDERS

    FROM segmented
    LEFT JOIN orders ON segmented.customer_id = orders.CUSTOMER_ID

)

SELECT * FROM final