


-- Only clean up I am doing is to filter for just Landlord/Agent accounts and to remove trailing '_c' from field names
 SELECT
        id,
        company,
        associated_city_c AS associated_city,
        postal_code AS postcode,
        percentage_student_c AS total_student_portfolio,
    FROM
        {{ source('salesforce', 'lead') }}
