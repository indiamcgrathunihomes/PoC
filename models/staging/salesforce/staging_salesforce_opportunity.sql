-- Only clean up I am doing is to filter for just Landlord/Agent accounts and to remove trailing '_c' from field names 
SELECT
        id,
        name,
        account_id,
        stage_name,
        total_opportunity_quantity
     FROM
      {{ source('salesforce', 'opportunity') }}