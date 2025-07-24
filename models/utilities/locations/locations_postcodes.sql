with

    source as ( select * from  {{ source('locations', 'uk_postcodes') }}),

    snake_case_field_names_and_clean_timestamps as ( select  {{ select_fields_structured('locations', 'uk_postcodes') }} from source)

select *
from snake_case_field_names_and_clean_timestamps