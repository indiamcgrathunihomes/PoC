with

base_records as (
    select *
    from {{ ref ("base_salesforce__analytics_with_foreign_key_names") }}
),

excluding_deleted_records as (
    select * from base_records where is_deleted = false
),

snake_case_field_names_and_clean_timestamps as (
    -- Original command was using macro and then compiled output shown here for consistency and visibilty of SF Field Aliases
     -- Original command was:   select {% raw %}{{ select_fields_structured(ref('base_salesforce__analytics_with_foreign_key_names')) }}{% endraw %} from excluding_deleted_records

    select -- Identifiers,

        "ID" as base_salesforce__analytics_with_foreign_key_names_id,


        -- Foreign Keys,

        "CREATED_BY_ID" as created_by_id,
        "LAST_MODIFIED_BY_ID" as last_modified_by_id,


        -- Descriptive,

        "NAME" as name,


        -- Dates,

        cast("CREATED_DATE" as timestamp) as created_date,
        cast("LAST_MODIFIED_DATE" as timestamp) as last_modified_date,
        cast("SYSTEM_MODSTAMP" as timestamp) as system_modstamp,
        cast("LAST_VIEWED_DATE" as timestamp) as last_viewed_date,
        cast("LAST_REFERENCED_DATE" as timestamp) as last_referenced_date,
        cast("LIVE_PROPERTY_CHECK_C" as timestamp) as live_property_check,
        cast("_FIVETRAN_SYNCED" as timestamp) as fivetran_synced,


        -- Other Fields,

        "IS_DELETED" as is_deleted,
        "LANDLORD_C" as landlord,
        "CROSS_SELL_C" as cross_sell,
        "ENQUIRY_APR_C" as enquiry_apr,
        "ENQUIRY_AUG_C" as enquiry_aug,
        "ENQUIRY_DEC_C" as enquiry_dec,
        "ENQUIRY_FEB_C" as enquiry_feb,
        "ENQUIRY_JAN_C" as enquiry_jan,
        "ENQUIRY_JUL_C" as enquiry_jul,
        "ENQUIRY_JUN_C" as enquiry_jun,
        "ENQUIRY_MAR_C" as enquiry_mar,
        "ENQUIRY_MAY_C" as enquiry_may,
        "ENQUIRY_NOV_C" as enquiry_nov,
        "ENQUIRY_OCT_C" as enquiry_oct,
        "ENQUIRY_SEPT_C" as enquiry_sept,
        "ORDER_APR_C" as order_apr,
        "ORDER_AUG_C" as order_aug,
        "ORDER_DEC_C" as order_dec,
        "ORDER_FEB_C" as order_feb,
        "ORDER_JAN_C" as order_jan,
        "ORDER_JUL_C" as order_jul,
        "ORDER_JUN_C" as order_jun,
        "ORDER_MAR_C" as order_mar,
        "ORDER_MAY_C" as order_may,
        "ORDER_NOV_C" as order_nov,
        "ORDER_OCT_C" as order_oct,
        "ORDER_SEPT_C" as order_sept,
        "RE_SIGN_C" as re_sign,
        "REFERRAL_COMMISSION_C" as referral_commission,
        "TOTAL_ENQUIRIES_C" as total_enquiries,
        "TOTAL_ORDER_FORMS_C" as total_order_forms,
        "UNI_HOMES_C" as uni_homes,
        "YEAR_C" as year,
        "AGENT_BILLING_C" as agent_billing,
        "COMMISSION_DEDUCTION_C" as commission_deduction,
        "TARGET_THIS_SEASON_C" as target_this_season,
        "LIVE_PROPERTIES_ON_WEBSITE_C" as live_properties_on_website,
        "AVERAGE_CONTRACT_LENGTH_C" as average_contract_length,
        "CANCELLATIONS_C" as cancellations,
        "GROUP_SIZE_MEAN_AVERAGE_C" as group_size_mean_average,
        "GROUP_SIZE_MODE_AVERAGE_C" as group_size_mode_average,
        "TOTAL_ORDER_FORM_WITH_CANCELLATION_C"
            as total_order_form_with_cancellation,
        "LEADS_APR_C" as leads_apr,
        "LEADS_AUG_C" as leads_aug,
        "LEADS_DEC_C" as leads_dec,
        "LEADS_FEB_C" as leads_feb,
        "LEADS_JAN_C" as leads_jan,
        "LEADS_JUL_C" as leads_jul,
        "LEADS_JUN_C" as leads_jun,
        "LEADS_MAR_C" as leads_mar,
        "LEADS_MAY_C" as leads_may,
        "LEADS_NOV_C" as leads_nov,
        "LEADS_OCT_C" as leads_oct,
        "LEADS_SEP_C" as leads_sep,
        "TOTAL_LEADS_C" as total_leads,
        "CURRENT_LIVE_PROPERTIES_C" as current_live_properties,
        "YEAR_RANK_C" as year_rank,
        "LIVE_PROPERTIES_IN_SEASON_C" as live_properties_in_season,
        "LETTING_SEASON_SPEND_ON_BOOSTS_C" as letting_season_spend_on_boosts,
        "LETTING_SEASON_SPEND_ON_FEATURED_PROPERT_C"
            as letting_season_spend_on_featured_propert,
        "UNIQUE_BOOSTS_PURCHASED_C" as unique_boosts_purchased,
        "UNIQUE_FEATURED_PROPERTIES_PURCHASED_C"
            as unique_featured_properties_purchased,
        "E_SIGN_C" as e_sign,
        "_FIVETRAN_DELETED" as fivetran_deleted,
        "ACCOUNT_NAME" as account_name,
        "CREATED_BY_NAME" as created_by_name,
        "LAST_MODIFIED_BY_NAME" as last_modified_by_name
    from excluding_deleted_records

)

select *
from snake_case_field_names_and_clean_timestamps
