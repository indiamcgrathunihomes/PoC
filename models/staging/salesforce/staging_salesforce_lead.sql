


-- Only clean up I am doing is remove trailing '_c' from field names
 SELECT
        id,
        company,
        associated_city_c AS associated_city,
        postal_code,
        percentage_student_c AS percentage_student,
        record_type_id,
        main_contact_c AS main_contact,
        status
    FROM
        {{ source('salesforce', 'lead') }}
