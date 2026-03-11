with final AS (
    SELECT
        id,
        email,
        created_at
    FROM customersource
)
    SELECT
        id,
        email,
        created_at
    FROM final