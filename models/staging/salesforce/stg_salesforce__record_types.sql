with

    source as (select * from {{ source("salesforce", "record_type") }}),

    -- We do not have the is_deleted field for this object so there is no CTE to
    -- filter out deleted records
    snake_case_field_names_and_clean_timestamps as (
        -- Original command was using macro and then compiled output shown here for
        -- consistency and visibilty of SF Field Aliases
        -- Original command was:   select {% raw %}{{
        -- select_fields_structured('salesforce', 'opportunity') }}{% endraw %} from
        -- excluding_deleted_records
        select
            "ID" as record_type_id,
            "BUSINESS_PROCESS_ID" as business_process_id,
            "CREATED_BY_ID" as created_by_id,
            "LAST_MODIFIED_BY_ID" as last_modified_by_id,
            "NAME" as name,
            "DESCRIPTION" as description,
            cast("CREATED_DATE" as timestamp) as created_date,
            cast("LAST_MODIFIED_DATE" as timestamp) as last_modified_date,
            cast("SYSTEM_MODSTAMP" as timestamp) as system_modstamp,
            cast("_FIVETRAN_SYNCED" as timestamp) as fivetran_synced,
            "DEVELOPER_NAME" as developer_name,
            "NAMESPACE_PREFIX" as namespace_prefix,
            "SOBJECT_TYPE" as sobject_type,
            "IS_ACTIVE" as is_active,
            "_FIVETRAN_DELETED" as fivetran_deleted
        from source

    )

select *
from snake_case_field_names_and_clean_timestamps
