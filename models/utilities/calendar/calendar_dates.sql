with

    source as ( select * from  {{ source('calendar', 'dates') }}),

    snake_case_field_names_and_clean_timestamps as ( select  {{ select_fields_structured('calendar', 'dates') }} from source)

select *
from snake_case_field_names_and_clean_timestamps