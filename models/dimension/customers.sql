with final AS (
    SELECT
        id,
        email,
        created_at,
        -- [SCRUM-20] COMPUTED: Added by schema-change agent
        LEFT(src.CUSTOMERCITY, 7) AS CUSTOMERCITY
    FROM customersource src
)
    SELECT
        id,
        email,
        created_at,
        CUSTOMERCITY
    FROM final