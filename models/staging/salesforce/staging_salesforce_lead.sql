


-- Only clean up I am doing is to filter for just Landlord/Agent accounts and to remove trailing '_c' from field names
 SELECT
        id,
        company,
        associated_city_c AS associated_city,
        postal_code AS postcode,
        percentage_student_c AS total_student_portfolio,
        record_type_id,
        main_contact_c AS main_contact,
        status
    FROM
        {{ source('salesforce', 'lead') }}
