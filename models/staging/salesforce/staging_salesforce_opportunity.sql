-- Only clean up I am doing is to filter for just Landlord/Agent accounts and to remove trailing '_c' from field names
SELECT
    id,
    name,
    account_id,
    stage_name
FROM 
    {{ source('salesforce', 'opportunity') }}
