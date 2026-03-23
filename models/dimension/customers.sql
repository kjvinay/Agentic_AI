with final AS (
    SELECT
        customer_id,
        customername,
        cusomeremail,
        customercountry,
        customerstate,
        customercity,
        -- [SCRUM-30] DIRECT: Added by schema-change agent
        src.CUSTOMERZIPCODE AS CUSTOMERZIPCODE,
        '1900-01-01' as created_at,
        COUNT(*) OVER () AS CUSTOMERCOUNT
    FROM src_customers src
)
    SELECT
        customer_id,
        customername,
        cusomeremail,
        customercountry,
        customerstate,
        customercity,
        CUSTOMERZIPCODE,
        created_at,
        COUNT(*) OVER () AS CUSTOMERCOUNT
    FROM final