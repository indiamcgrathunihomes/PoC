-- Removal of trailing '_c' from field names
-- Filtering out deleted records
SELECT
    id,
    opportunity_id,
    portfolio_type_c AS portfolio_type,
    quantity
FROM 
     {{ source('salesforce', 'opportunity_products') }}
WHERE
    is_deleted = FALSE