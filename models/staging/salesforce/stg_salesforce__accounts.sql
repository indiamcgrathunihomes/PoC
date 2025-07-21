with

    source as ( select * from {{ source("salesforce", "account") }}),

    excluding_deleted_records as (select * from source where is_deleted = false),

    snake_case_field_names_and_clean_timestamps as ( select  {{ select_fields_structured('salesforce', 'account') }} from excluding_deleted_records)

select *
from snake_case_field_names_and_clean_timestamps
