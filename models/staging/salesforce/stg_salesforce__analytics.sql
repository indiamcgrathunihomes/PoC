with

    base_records as (select * from {{ ref ("base_salesforce__analytics_with_foreign_key_names") }} ), 

    excluding_deleted_records as (select * from base_records where is_deleted = false),

    snake_case_field_names_and_clean_timestamps as (
        -- Original command was using macro and then compiled output shown here for consistency and visibilty of SF Field Aliases
        -- Original command was:   select {% raw %}{{ select_fields_structured(ref('base_salesforce__analytics_with_foreign_key_names')) }}{% endraw %} from excluding_deleted_records
 
 select {{ select_fields_structured(ref('base_salesforce__analytics_with_foreign_key_names')) }} from excluding_deleted_records

    )

select *
from snake_case_field_names_and_clean_timestamps
