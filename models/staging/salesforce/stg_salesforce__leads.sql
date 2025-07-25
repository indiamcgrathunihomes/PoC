with

    base_records as (select * from {{ ref ("base_salesforce__leads_with_foreign_key_names") }} ), 

    excluding_deleted_records as (select * from base_records where is_deleted = false),

    snake_case_field_names_and_clean_timestamps as (
        -- Original command was using macro and then compiled output shown here for consistency and visibilty of SF Field Aliases
        -- Original command was:   select {% raw %}{{ select_fields_structured(ref('base_salesforce__leads_with_foreign_key_names')) }}{% endraw %} from excluding_deleted_records
       
          
            
   select 

-- Identifiers,

    "ID" as lead_id, -- Automatically macro produces different name based on ref. Corrected here.

    
-- Foreign Keys,

    "MASTER_RECORD_ID" as master_record_id,
    "RECORD_TYPE_ID" as record_type_id,
    "OWNER_ID" as owner_id,
    "CONVERTED_ACCOUNT_ID" as converted_account_id,
    "CONVERTED_CONTACT_ID" as converted_contact_id,
    "CONVERTED_OPPORTUNITY_ID" as converted_opportunity_id,
    "CREATED_BY_ID" as created_by_id,
    "LAST_MODIFIED_BY_ID" as last_modified_by_id,
    "JIGSAW_CONTACT_ID" as jigsaw_contact_id,
    "INDIVIDUAL_ID" as individual_id,
    "ACTIVITY_METRIC_ID" as activity_metric_id,
    "ACTIVITY_METRIC_ROLLUP_ID" as activity_metric_rollup_id,

    
-- Descriptive,

    "NAME" as name,
    "INDUSTRY" as industry,

    
-- Dates,

    cast("CONVERTED_DATE" as date) as converted_date,
    cast("CREATED_DATE" as timestamp) as created_date,
    cast("LAST_MODIFIED_DATE" as timestamp) as last_modified_date,
    cast("SYSTEM_MODSTAMP" as timestamp) as system_modstamp,
    cast("LAST_ACTIVITY_DATE" as date) as last_activity_date,
    cast("LAST_VIEWED_DATE" as timestamp) as last_viewed_date,
    cast("LAST_REFERENCED_DATE" as timestamp) as last_referenced_date,
    cast("EMAIL_BOUNCED_DATE" as timestamp) as email_bounced_date,
    cast("LAST_TEXT_SENT_C" as timestamp) as last_text_sent,
    cast("DATE_OF_LAST_ENQUIRY_C" as date) as date_of_last_enquiry,
    cast("DATE_OF_BIRTH_C" as date) as date_of_birth,
    cast("DOCU_SIGN_SENT_DATE_C" as date) as docu_sign_sent_date,
    cast("PROPERTY_END_DATE_C" as date) as property_end_date,
    cast("PROPERTY_START_DATE_C" as date) as property_start_date,
    cast("DATE_OF_INTEREST_C" as date) as date_of_interest,
    cast("LAST_PROPERTY_EMAIL_SENT_C" as timestamp) as last_property_email_sent,
    cast("OTHER_AGENT_TENANCY_START_MONTH_C" as date) as other_agent_tenancy_start_month,
    cast("FIRST_ACQUISITION_DATE_C" as date) as first_acquisition_date,
    cast("CALLBACK_DATE_TIME_C" as timestamp) as callback_date_time,
    cast("CALLBACK_DATE_C" as date) as callback_date,
    cast("COMPANY_REGISTRATION_DATE_C" as date) as company_registration_date,
    cast("COMPETITOR_CONTRACT_END_DATE_C" as date) as competitor_contract_end_date,
    cast("DATE_OF_LAST_CONTACT_C" as date) as date_of_last_contact,
    cast("DATE_OF_RE_APPROACH_C" as date) as date_of_re_approach,
    cast("LAST_COMMUNICATION_DATE_TIME_C" as timestamp) as last_communication_date_time,
    cast("_FIVETRAN_SYNCED" as timestamp) as fivetran_synced,

    
-- Other Fields,

    "IS_DELETED" as is_deleted,
    "LAST_NAME" as last_name,
    "FIRST_NAME" as first_name,
    "SALUTATION" as salutation,
    "TITLE" as title,
    "COMPANY" as company,
    "STREET" as street,
    "CITY" as city,
    "STATE" as state,
    "POSTAL_CODE" as postal_code,
    "COUNTRY" as country,
    "LATITUDE" as latitude,
    "LONGITUDE" as longitude,
    "GEOCODE_ACCURACY" as geocode_accuracy,
    "PHONE" as phone,
    "MOBILE_PHONE" as mobile_phone,
    "EMAIL" as email,
    "WEBSITE" as website,
    "PHOTO_URL" as photo_url,
    "LEAD_SOURCE" as lead_source,
    "STATUS" as status,
    "NUMBER_OF_EMPLOYEES" as number_of_employees,
    "HAS_OPTED_OUT_OF_EMAIL" as has_opted_out_of_email,
    "IS_CONVERTED" as is_converted,
    "IS_UNREAD_BY_OWNER" as is_unread_by_owner,
    "JIGSAW" as jigsaw,
    "EMAIL_BOUNCED_REASON" as email_bounced_reason,
    "IS_PRIORITY_RECORD" as is_priority_record,
    "LANDLORD_AGENT_C" as landlord_agent,
    "GROUP_SIZE_C" as group_size,
    "EMAILS_QUEUED_C" as emails_queued,
    "LAST_PROPERTY_ENQUIRY_C" as last_property_enquiry,
    "BLOCKER_REASON_C" as blocker_reason,
    "LOST_REASON_C" as lost_reason,
    "DATABASE_C" as database,
    "WEEKLY_PRICE_C" as weekly_price,
    "PROPERTY_SEARCH_STATUS_C" as property_search_status,
    "DOCU_SIGN_SENT_C" as docu_sign_sent,
    "ARCHIVED_REASON_C" as archived_reason,
    "LEAD_CONTACT_C" as lead_contact,
    "PROPERTY_SIGNED_FOR_C" as property_signed_for,
    "SIGNED_ALL_INCLUSIVE_C" as signed_all_inclusive,
    "DOCUSIGN_RECIPIENT_STATUS_C" as docusign_recipient_status,
    "PROPERTY_ADDRESS_C" as property_address,
    "LETTING_AGENT_C" as letting_agent,
    "PROPERTY_TOWN_C" as property_town,
    "NUMBER_OF_TENANTS_C" as number_of_tenants,
    "SEND_3_RD_EMAIL_SMS_C" as send_3_rd_email_sms,
    "SEND_4_TH_EMAIL_SMS_C" as send_4_th_email_sms,
    "SEND_5_TH_EMAIL_SMS_C" as send_5_th_email_sms,
    "SEND_INITIAL_SIGNUP_SMS_C" as send_initial_signup_sms,
    "SEND_SIGNUP_REMINDER_SMS_C" as send_signup_reminder_sms,
    "CUSTOMER_TYPE_C" as customer_type,
    "INTERNET_PACKAGE_C" as internet_package,
    "PROPERTY_SIGNUP_REQUEST_CANCELLED_C" as property_signup_request_cancelled,
    "PROPERTY_SIGNUP_REQUEST_OUTSTANDING_C" as property_signup_request_outstanding,
    "SEND_1_ST_EMAIL_SMS_C" as send_1_st_email_sms,
    "WATER_C" as water,
    "TV_LICENCE_C" as tv_licence,
    "SEND_1_ST_SIGNUP_REMINDER_SMS_C" as send_1_st_signup_reminder_sms,
    "SEND_2_ND_SIGNUP_REMINDER_SMS_C" as send_2_nd_signup_reminder_sms,
    "GAS_C" as gas,
    "DOCUSIGN_VIEWED_C" as docusign_viewed,
    "ELECTRICITY_C" as electricity,
    "ET_4_AE_5_HAS_OPTED_OUT_OF_MOBILE_C" as et_4_ae_5_has_opted_out_of_mobile,
    "ET_4_AE_5_MOBILE_COUNTRY_CODE_C" as et_4_ae_5_mobile_country_code,
    "NUMBER_OF_ENQUIRIES_C" as number_of_enquiries,
    "X_1_ST_EMAIL_C" as x_1_st_email,
    "X_2_ND_EMAIL_C" as x_2_nd_email,
    "X_1_ST_CALL_C" as x_1_st_call,
    "X_2_ND_CALL_C" as x_2_nd_call,
    "X_3_RD_EMAIL_C" as x_3_rd_email,
    "X_3_RD_CALL_C" as x_3_rd_call,
    "INTERESTED_IN_UTILITIES_C" as interested_in_utilities,
    "LOST_PROVIDER_C" as lost_provider,
    "BUYING_STAGE_C" as buying_stage,
    "NOTE_C" as note,
    "ENERGY_WEEKLY_COST_PP_C" as energy_weekly_cost_pp,
    "BROADBAND_WEEKLY_COST_PP_C" as broadband_weekly_cost_pp,
    "NON_DISCOUNTED_WEEKLY_PRICE_C" as non_discounted_weekly_price,
    "PACKAGE_SHARE_LINK_C" as package_share_link,
    "TV_LICENCE_WEEKLY_COST_PP_C" as tv_licence_weekly_cost_pp,
    "WATER_WEEKLY_COST_PP_C" as water_weekly_cost_pp,
    "SALES_INCENTIVE_C" as sales_incentive,
    "NON_DISCOUNTED_ENERGY_WEEKLY_COST_PP_C" as non_discounted_energy_weekly_cost_pp,
    "NON_DISCOUNTED_BROADBAND_WEEKLY_COST_PP_C" as non_discounted_broadband_weekly_cost_pp,
    "NON_DISCOUNTED_WATER_WEEKLY_COST_PP_C" as non_discounted_water_weekly_cost_pp,
    "NON_DISCOUNTED_TV_LICENCE_WEEKLY_COST_PP_C" as non_discounted_tv_licence_weekly_cost_pp,
    "CHOSEN_ENQUIRY_C" as chosen_enquiry,
    "NO_HOT_PROPERTIES_C" as no_hot_properties,
    "NO_NEW_PROPERTIES_C" as no_new_properties,
    "LANDLORD_AGENT_ACCOUNT_C" as landlord_agent_account,
    "CONTACT_LINK_C" as contact_link,
    "UTILITY_ORDER_FORM_ACCOUNT_C" as utility_order_form_account,
    "PROPERTY_COUNT_C" as property_count,
    "CAMPAIGN_SOURCE_C" as campaign_source,
    "FIRST_ACQUISITION_CAMPAIGN_C" as first_acquisition_campaign,
    "FIRST_ACQUISITION_SOURCE_C" as first_acquisition_source,
    "CAMPAIGN_MONITOR_EXCLUDED_LETTING_AGENTS_C" as campaign_monitor_excluded_letting_agents,
    "FIRST_ACQUISITION_MEDIUM_C" as first_acquisition_medium,
    "FORMATTED_MOBILE_PHONE_C" as formatted_mobile_phone,
    "RERUN_ASSIGNMENT_C" as rerun_assignment,
    "SPLIT_THE_BILLS_MOBILE_OPT_OUT_C" as split_the_bills_mobile_opt_out,
    "VERIFIED_TRUST_PILOT_LINK_C" as verified_trust_pilot_link,
    "LEAD_TYPE_C" as lead_type,
    "AREA_OF_INTEREST_C" as area_of_interest,
    "ASSOCIATED_CITY_C" as associated_city,
    "BILLING_MODEL_C" as billing_model,
    "BILLS_INCLUDED_C" as bills_included,
    "BLOCKERS_C" as blockers,
    "CALLBACK_TIME_C" as callback_time,
    "COMPANY_STATUS_C" as company_status,
    "COMPETITOR_COMMISSION_PER_TENANT_C" as competitor_commission_per_tenant,
    "COMPETITOR_NAME_C" as competitor_name,
    "CONTACT_TYPE_C" as contact_type,
    "COUNT_OF_CALLS_MADE_ON_CONVERSION_C" as count_of_calls_made_on_conversion,
    "COUNT_OF_CLOSED_TASKS_ON_CONVERSION_C" as count_of_closed_tasks_on_conversion,
    "COUNT_OF_EMAILS_SENT_ON_CONVERSION_C" as count_of_emails_sent_on_conversion,
    "COUNT_OF_OPEN_TASKS_ON_CONVERSION_C" as count_of_open_tasks_on_conversion,
    "FORCE_ACTIVITY_LINK_C" as force_activity_link,
    "E_SIGN_TOOL_USED_C" as e_sign_tool_used,
    "HOW_DID_YOU_HEAR_ABOUT_US_C" as how_did_you_hear_about_us,
    "LETTING_TYPE_C" as letting_type,
    "MAIN_CONTACT_C" as main_contact,
    "OFFICE_EMAIL_C" as office_email,
    "OFFICE_PHONE_NUMBER_C" as office_phone_number,
    "PARTNER_AGENT_REFERRED_TO_C" as partner_agent_referred_to,
    "PORTFOLIO_SIZE_C" as portfolio_size,
    "PREVIOUS_RELATIONSHIPS_C" as previous_relationships,
    "REGISTERED_COMPANY_NAME_C" as registered_company_name,
    "REGISTERED_COMPANY_NUMBER_C" as registered_company_number,
    "REQUIRE_ADVERTISING_C" as require_advertising,
    "UNQUALIFIED_REASON_OTHER_C" as unqualified_reason_other,
    "OF_PORTFOLIO_LET_C" as of_portfolio_let,
    "DISQUALIFIED_REASON_C" as disqualified_reason,
    "UNQUALIFIED_REASON_C" as unqualified_reason,
    "LISTINGS_C" as listings,
    "PERCENTAGE_PROFESSIONAL_C" as percentage_professional,
    "PERCENTAGE_STUDENT_C" as percentage_student,
    "TENANCY_SIGN_UP_PROCESS_C" as tenancy_sign_up_process,
    "TENANCY_SOFTWARE_TOOL_USED_C" as tenancy_software_tool_used,
    "UTILITY_FORM_SIGN_UP_PROCESS_C" as utility_form_sign_up_process,
    "VOUCHER_CODE_USED_C" as voucher_code_used,
    "VALIDATED_AUTHENTICITY_C" as validated_authenticity,
    "DRIVERS_C" as drivers,
    "TEMP_FIELD_C" as temp_field,
    "REGISTERED_STREET_C" as registered_street,
    "REGISTERED_CITY_C" as registered_city,
    "REGISTERED_REGION_C" as registered_region,
    "REGISTERED_COUNTRY_C" as registered_country,
    "REGISTERED_POSTCODE_C" as registered_postcode,
    "HIGH_PRIORITY_C" as high_priority,
    "LEAD_SCORE_C" as lead_score,
    "REASON_FOR_NOT_SIGNING_BILLS_INCLUSIVE_C" as reason_for_not_signing_bills_inclusive,
    "OTHER_DETAILS_C" as other_details,
    "OTHER_BLOCKER_DESCRIPTION_C" as other_blocker_description,
    "BTR_PBSA_C" as btr_pbsa,
    "FUEL_INCLUDED_C" as fuel_included,
    "BROADBAND_INCLUDED_C" as broadband_included,
    "TV_LICENSE_INCLUDED_C" as tv_license_included,
    "WATER_INCLUDED_C" as water_included,
    "MARKETING_SOURCE_C" as marketing_source,
    "MARKETING_QUALIFIED_C" as marketing_qualified,
    "PRIMARY_CONTACT_FLAG_C" as primary_contact_flag,
    "EMAIL_DELIVERABILITY_C" as email_deliverability,
    "EMAIL_VALIDATION_C" as email_validation,
    "EMAIL_VALIDATION_ERROR_MESSAGE_C" as email_validation_error_message,
    "_FIVETRAN_DELETED" as fivetran_deleted,
    "RECYCLED_CONTACT_C" as recycled_contact,
    "RECYCLED_ACCOUNT_C" as recycled_account,
    "PARENT_LEAD_C" as parent_lead,
    "MASTER_RECORD_NAME" as master_record_name,
    "RECORD_TYPE_NAME" as record_type_name,
    "OWNER_NAME" as owner_name,
    "CREATED_BY_NAME" as created_by_name,
    "LAST_MODIFIED_BY_NAME" as last_modified_by_name,
    "CONVERTED_ACCOUNT_NAME" as converted_account_name,
    "CONVERTED_OPPORTUNITY_NAME" as converted_opportunity_name
    from excluding_deleted_records

    )

select *
from snake_case_field_names_and_clean_timestamps
