with final AS (
    SELECT
        customer_id,
        customername,
        cusomeremail,
        customercountry,
        customerstate,
        customercity,
        '1900-01-01' as created_at,
        COUNT(*) OVER () AS CUSTOMERCOUNT,
        -- [SCRUM-28] COMPUTED: Added by schema-change agent
        CASE 
            WHEN COUNT(*) OVER () < 250 THEN 'small'
            WHEN COUNT(*) OVER () BETWEEN 250 AND 500 THEN 'midsize'
            WHEN COUNT(*) OVER () > 500 THEN 'enterprise'
            ELSE NULL
        END AS CUSTOMERSEGMENT
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
        COUNT(*) OVER () AS CUSTOMERCOUNT,
        CUSTOMERSEGMENT
    FROM final