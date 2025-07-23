with

    source as (select * from {{ source("salesforce", "opportunity_product") }}),

    excluding_deleted_records as (select * from source where is_deleted = false),

    snake_case_field_names_and_clean_timestamps as (
        -- Original command was using macro and then compiled output shown here for consistency and visibilty of SF Field Aliases
        -- Original command was:   select {% raw %}{{ select_fields_structured('salesforce', 'opportunity') }}{% endraw %} from excluding_deleted_records
        select
            "ID" as opportunity_product_id,
            "OPPORTUNITY_ID" as opportunity_id,
            "PRICEBOOK_ENTRY_ID" as pricebook_entry_id,
            "PRODUCT_2_ID" as product_2_id,
            "CREATED_BY_ID" as created_by_id,
            "LAST_MODIFIED_BY_ID" as last_modified_by_id,
            "NAME" as name,
            "DESCRIPTION" as description,
            cast("SERVICE_DATE" as date) as service_date,
            cast("CREATED_DATE" as timestamp) as created_date,
            cast("LAST_MODIFIED_DATE" as timestamp) as last_modified_date,
            cast("SYSTEM_MODSTAMP" as timestamp) as system_modstamp,
            cast("LAST_VIEWED_DATE" as timestamp) as last_viewed_date,
            cast("LAST_REFERENCED_DATE" as timestamp) as last_referenced_date,
            cast("_FIVETRAN_SYNCED" as timestamp) as fivetran_synced,
            "SORT_ORDER" as sort_order,
            "PRODUCT_CODE" as product_code,
            "QUANTITY" as quantity_0,
            "TOTAL_PRICE" as total_price,
            "UNIT_PRICE" as unit_price,
            "LIST_PRICE" as list_price,
            "IS_DELETED" as is_deleted,
            "CITY_C" as city,
            "QUANTITY_C" as quantity_1,
            "PORTFOLIO_TYPE_C" as portfolio_type,
            "_FIVETRAN_DELETED" as fivetran_deleted
        from excluding_deleted_records
    )

select *
from snake_case_field_names_and_clean_timestamps
