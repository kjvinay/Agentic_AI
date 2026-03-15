with final AS (
    SELECT
        id,
        email,
        created_at,
        -- [SCRUM-15] AGGREGATE: Added by schema-change agent
        COUNT(src.customer_id) AS customercount
    FROM customersource src
    GROUP BY customer_id
)
    SELECT
        id,
        email,
        created_at,
        customercount
    FROM final