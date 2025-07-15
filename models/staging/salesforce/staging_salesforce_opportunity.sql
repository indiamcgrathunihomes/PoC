-- Only clean up I am doing is to filter for just Landlord/Agent accounts and to remove trailing '_c' from field names 
SELECT
        id,
        name,
        associated_city_c AS associated_city,
        billing_postal_code,
        total_student_portfolio_c AS total_student_portfolio,
        account_type_c AS account_type,
        account_id,
        stage_name
     FROM
      {{ source('salesforce', 'opportunity') }}