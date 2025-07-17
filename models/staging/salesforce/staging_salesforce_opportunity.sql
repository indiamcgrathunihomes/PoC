-- Removal of trailing '_c' from field names
-- Filtering out deleted records
SELECT
    id,
    name,
    account_id,
    stage_name,
    competitor_name_c AS competitor,
    record_type_id
FROM 
    {{ source('salesforce', 'opportunity') }}
WHERE
    is_deleted = FALSE