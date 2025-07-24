with

    base_records as (select * from {{ ref ("base_salesforce__opportunities_with_foreign_key_names") }} ), 

    excluding_deleted_records as (select * from base_records where is_deleted = false),

    snake_case_field_names_and_clean_timestamps as (
        -- Original command was using macro and then compiled output shown here for consistency and visibilty of SF Field Aliases
        -- Original command was:   select {% raw %}{{ select_fields_structured(ref('base_salesforce__opportunities_with_foreign_key_names')) }}{% endraw %} from excluding_deleted_records
       
          
   select -- Identifiers,

    "ID" as opportunity_id, -- Automatically macro produces different name based on ref. Corrected here.

    
-- Foreign Keys,

    "ACCOUNT_ID" as account_id,
    "RECORD_TYPE_ID" as record_type_id,
    "CAMPAIGN_ID" as campaign_id,
    "PRICEBOOK_2_ID" as pricebook_2_id,
    "OWNER_ID" as owner_id,
    "CREATED_BY_ID" as created_by_id,
    "LAST_MODIFIED_BY_ID" as last_modified_by_id,
    "CONTACT_ID" as contact_id,
    "SYNCED_QUOTE_ID" as synced_quote_id,
    "LAST_AMOUNT_CHANGED_HISTORY_ID" as last_amount_changed_history_id,
    "LAST_CLOSE_DATE_CHANGED_HISTORY_ID" as last_close_date_changed_history_id,
    "ACTIVITY_METRIC_ID" as activity_metric_id,
    "ACTIVITY_METRIC_ROLLUP_ID" as activity_metric_rollup_id,

    
-- Descriptive,

    "NAME" as name,
    "DESCRIPTION" as description,
    "TYPE" as type,

    
-- Dates,

    cast("CLOSE_DATE" as date) as close_date,
    cast("CREATED_DATE" as timestamp) as created_date,
    cast("LAST_MODIFIED_DATE" as timestamp) as last_modified_date,
    cast("SYSTEM_MODSTAMP" as timestamp) as system_modstamp,
    cast("LAST_ACTIVITY_DATE" as date) as last_activity_date,
    cast("LAST_STAGE_CHANGE_DATE" as timestamp) as last_stage_change_date,
    cast("LAST_VIEWED_DATE" as timestamp) as last_viewed_date,
    cast("LAST_REFERENCED_DATE" as timestamp) as last_referenced_date,
    cast("AGREEMENT_SENT_DATE_C" as date) as agreement_sent_date,
    cast("AGREEMENT_SIGNED_DATE_C" as date) as agreement_signed_date,
    cast("COMPETITOR_CONTRACT_END_DATE_C" as date) as competitor_contract_end_date,
    cast("CONTRACT_END_DATE_C" as date) as contract_end_date,
    cast("CONTRACT_START_DATE_C" as date) as contract_start_date,
    cast("GO_LIVE_CALL_Y_2_C" as date) as go_live_call_y_2,
    cast("GO_LIVE_CALL_YEAR_1_C" as date) as go_live_call_year_1,
    cast("VOIDED_DATE_C" as date) as voided_date,
    cast("CALLBACK_DATE_TIME_C" as timestamp) as callback_date_time,
    cast("_FIVETRAN_SYNCED" as timestamp) as fivetran_synced,

    
-- Measures,

    "AMOUNT" as amount,

    
-- Other Fields,

    "IS_DELETED" as is_deleted,
    "STAGE_NAME" as stage_name,
    "PROBABILITY" as probability,
    "TOTAL_OPPORTUNITY_QUANTITY" as total_opportunity_quantity,
    "NEXT_STEP" as next_step,
    "LEAD_SOURCE" as lead_source,
    "IS_CLOSED" as is_closed,
    "IS_WON" as is_won,
    "FORECAST_CATEGORY" as forecast_category,
    "FORECAST_CATEGORY_NAME" as forecast_category_name,
    "HAS_OPPORTUNITY_LINE_ITEM" as has_opportunity_line_item,
    "PUSH_COUNT" as push_count,
    "FISCAL_QUARTER" as fiscal_quarter,
    "FISCAL_YEAR" as fiscal_year,
    "FISCAL" as fiscal,
    "HAS_OPEN_ACTIVITY" as has_open_activity,
    "HAS_OVERDUE_TASK" as has_overdue_task,
    "ADVERTISING_PLATFORMS_C" as advertising_platforms,
    "AGREEMENT_SIGNED_BY_C" as agreement_signed_by,
    "AGREEMENT_STATUS_C" as agreement_status,
    "APPROACHED_BY_COMPETITOR_C" as approached_by_competitor,
    "AREA_OF_INTEREST_C" as area_of_interest,
    "ASSOCIATED_CITY_C" as associated_city,
    "BILLING_MODEL_C" as billing_model,
    "BILLS_INCLUDED_C" as bills_included,
    "CRM_PROVIDER_C" as crm_provider,
    "COMPETITOR_COMMISSION_PER_TENANT_C" as competitor_commission_per_tenant,
    "COMPETITOR_NAME_C" as competitor_name,
    "DRIVERS_C" as drivers,
    "FEED_PROVIDER_C" as feed_provider,
    "LETTING_TYPE_C" as letting_type,
    "PERCENTAGE_OF_PORTFOLIO_LET_C" as percentage_of_portfolio_let,
    "PORTFOLIO_SIZE_C" as portfolio_size,
    "REQUIRE_ADVERTISING_C" as require_advertising,
    "VOID_REASON_C" as void_reason,
    "LOST_REASON_C" as lost_reason,
    "COUNT_C" as count,
    "MAXIMUM_NUMBER_OF_LIVE_PROPERTIES_C" as maximum_number_of_live_properties,
    "SPECIFIC_TERMS_C" as specific_terms,
    "PERCENTAGE_PROFESSIONAL_C" as percentage_professional,
    "PERCENTAGE_STUDENT_C" as percentage_student,
    "CONTACT_TYPE_C" as contact_type,
    "BLOCKERS_C" as blockers,
    "PREVIOUS_RELATIONSHIP_C" as previous_relationship,
    "CONTRACT_LENGTH_C" as contract_length,
    "MOBILE_PHONE_C" as mobile_phone,
    "SPECIAL_CONDITION_C" as special_condition,
    "LISTINGS_C" as listings,
    "LOST_REASON_DESCRIPTION_C" as lost_reason_description,
    "PORTFOLIO_NOTES_C" as portfolio_notes,
    "REALISTIC_SHARER_PORTFOLIO_C" as realistic_sharer_portfolio,
    "REALISTIC_STUDENT_PORTFOLIO_C" as realistic_student_portfolio,
    "OTHER_BLOCKER_DESCRIPTION_C" as other_blocker_description,
    "HIGH_PRIORITY_C" as high_priority,
    "AUTO_RENEWAL_C" as auto_renewal,
    "BTR_PBSA_C" as btr_pbsa,
    "FUEL_INCLUDED_C" as fuel_included,
    "BROADBAND_INCLUDED_C" as broadband_included,
    "COMMISION_PERCENTAGE_C" as commision_percentage,
    "NUMBER_OF_BUILDINGS_C" as number_of_buildings,
    "TV_LICENSE_INCLUDED_C" as tv_license_included,
    "TOTAL_UNITS_C" as total_units,
    "WATER_INCLUDED_C" as water_included,
    "MARKETING_SOURCE_C" as marketing_source,
    "MARKETING_QUALIFIED_C" as marketing_qualified,
    "CURRENT_ENGAGEMENT_LEVEL_C" as current_engagement_level,
    "KEY_PERFORMANCE_INDICATORS_C" as key_performance_indicators,
    "MARKETING_CONTRIBUTION_C" as marketing_contribution,
    "PARTNER_SATISFACTION_RATING_C" as partner_satisfaction_rating,
    "PARTNERSHIP_STATUS_C" as partnership_status,
    "ADVERTISING_OPPORTUNITIES_C" as advertising_opportunities,
    "EXCLUSIVITY_TERMS_C" as exclusivity_terms,
    "COMMITTED_TO_DELIVER_EXPECTED_CONTRACTS_C" as committed_to_deliver_expected_contracts,
    "AGENT_UNDERSTANDS_TO_RETURN_CONTRACTS_C" as agent_understands_to_return_contracts,
    "EXPECTED_CONTRACTS_TO_BE_RETURNED_C" as expected_contracts_to_be_returned,
    "TENANCY_SIGN_UP_PROCESS_C" as tenancy_sign_up_process,
    "UTILITY_FORM_SIGN_UP_PROCESS_C" as utility_form_sign_up_process,
    "ENQUIRY_TO_FORM_SEND_TIMEFRAME_DAYS_C" as enquiry_to_form_send_timeframe_days,
    "_FIVETRAN_DELETED" as fivetran_deleted,
    "CREATE_RENEWAL_C" as create_renewal,
    "RECYCLED_MAIN_LEAD_C" as recycled_main_lead,
    "ACCOUNT_NAME" as account_name,
    "RECORD_TYPE_NAME" as record_type_name,
    "OWNER_NAME" as owner_name,
    "CREATED_BY_NAME" as created_by_name,
    "LAST_MODIFIED_BY_NAME" as last_modified_by_name
    from excluding_deleted_records

    )

select *
from snake_case_field_names_and_clean_timestamps
