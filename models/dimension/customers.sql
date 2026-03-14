with final AS (
    SELECT
        id,
        email,
        created_at,
        -- [SCRUM-11] Added by schema-change agent
        CUSTOMERSTATE AS CUSTOMERSTATE
    FROM customersource
)
    SELECT
        id,
        email,
        created_at,
        CUSTOMERSTATE
    FROM final