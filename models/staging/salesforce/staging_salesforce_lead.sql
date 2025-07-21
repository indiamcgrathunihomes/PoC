-- Removal of trailing '_c' from field names
-- Filtering out deleted records
 SELECT
        id,
        name,
        company,
        associated_city_c AS associated_city,
        postal_code,
        percentage_student_c AS percentage_student,
        record_type_id,
        main_contact_c AS main_contact,
        status,
        phone,
        competitor_name_c AS competitor,
        email,
        created_date
    FROM
        {{ source('salesforce', 'lead') }}
    WHERE
        is_deleted = FALSE
