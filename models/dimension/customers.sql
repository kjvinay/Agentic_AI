with final AS (
    SELECT
        id,
        email,
        created_at,
        -- [SCRUM-14] Added by schema-change agent
        COUNT(*) AS CUSTOMERCOUNT
    FROM customersource
)
    SELECT
        id,
        email,
        created_at,
        CUSTOMERCOUNT
    FROM final