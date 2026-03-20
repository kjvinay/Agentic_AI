with final AS (
    SELECT
        customer_id,
        customername,
        cusomeremail,
        customercountry,
        customerstate,
        customercity,
        '1900-01-01' as created_at,
        -- [SCRUM-13] Added by schema-change agent
        COUNT(*) OVER () AS CUSTOMERCOUNT
    FROM src_customers
)
    SELECT
        customer_id,
        customername,
        cusomeremail,
        customercountry,
        customerstate,
        customercity,
        created_at,
        COUNT(*) OVER () AS CUSTOMERCOUNT
    FROM final
