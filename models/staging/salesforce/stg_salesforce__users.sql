with

    source as (select * from {{ source("salesforce", "user") }}),

    -- We do not have the is_deleted field for this object so there is no CTE to
    -- filter out deleted records
    snake_case_field_names_and_clean_timestamps as (
        -- Original command was using macro and then compiled output shown here for
        -- consistency and visibilty of SF Field Aliases
        -- Original command was:   select {% raw %}{{
        -- select_fields_structured('salesforce', 'user') }}{% endraw %} from
        -- source
        select select{{select_fields_structured('salesforce', 'user') }}
          
        from source

    )

select *
from snake_case_field_names_and_clean_timestamps
