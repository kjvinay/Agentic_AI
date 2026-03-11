with final AS (
    SELECT
        id,
        email,
        created_at,
        -- [DATA-E2E-002] Added by schema-change agent
        CUSTOMER_EMAIL AS CUSTOMEREMAIL
    FROM customersource
)
    SELECT
        id,
        email,
        created_at,
        CUSTOMEREMAIL
    FROM final