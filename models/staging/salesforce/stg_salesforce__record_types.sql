-- Removal of trailing '_c' from field names
SELECT
    id,
    name AS record_type
FROM
    {{ source('salesforce', 'record_types') }}