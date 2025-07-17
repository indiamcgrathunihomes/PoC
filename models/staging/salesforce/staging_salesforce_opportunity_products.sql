SELECT
    opportunity_id,
    SUM(quantity) as total_student_portfolio
FROM 
     {{ source('salesforce', 'opportunity_products') }}
WHERE
    portfolio_type_c = 'Student'
GROUP BY
    opportunity_id