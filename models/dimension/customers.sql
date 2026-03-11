with final AS (
    SELECT
        id,
        email,
        created_at,
        -- [DATA-E2E-001] Added by schema-change agent
        CUSTOMER_NAME AS CUSTOMERNAME
    FROM customersource
)
    SELECT
        id,
        email,
        created_at,
        CUSTOMERNAME
    FROM final