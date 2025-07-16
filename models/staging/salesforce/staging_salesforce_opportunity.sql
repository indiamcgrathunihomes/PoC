-- Only clean up I am doing is to filter for just Landlord/Agent accounts and to remove trailing '_c' from field names 
WITH opportunity_quantity AS (
    SELECT
        opportunity_id,
        SUM(quantity) as total_student_portfolio
    FROM 
        {{ source('salesforce', 'opportunity_products') }}
    WHERE
        portfolio_type_c = 'Student'
    GROUP BY
        opportunity_id
),

opportunity AS (
    SELECT
        id,
        name,
        account_id,
        stage_name
    FROM 
        {{ source('salesforce', 'opportunity') }}
)

SELECT
    op.id,
    op.name,
    op.account_id,
    op.stage_name,
    COALESCE(oq.total_student_portfolio,0) AS total_student_portfolio
FROM
    opportunity op
LEFT JOIN 
    opportunity_quantity oq ON op.id = oq.opportunity_id