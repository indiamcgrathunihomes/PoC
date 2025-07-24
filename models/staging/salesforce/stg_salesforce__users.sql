with

source as (select * from {{ source("salesforce", "user") }}),

-- We do not have the is_deleted field for this object so there is no CTE to
-- filter out deleted records
snake_case_field_names_and_clean_timestamps as (
    -- Original command was using macro and then compiled output shown here for
    -- consistency and visibilty of SF Field Aliases
    {% raw %}{{
    -- select_fields_structured('salesforce', 'user') }} from source
    {% endraw %}

    select
        "ID" as user_id,
        "USER_ROLE_ID" as user_role_id,
        "PROFILE_ID" as profile_id,
        "DELEGATED_APPROVER_ID" as delegated_approver_id,
        "MANAGER_ID" as manager_id,
        "CREATED_BY_ID" as created_by_id,
        "LAST_MODIFIED_BY_ID" as last_modified_by_id,
        "CONTACT_ID" as contact_id,
        "ACCOUNT_ID" as account_id,
        "CALL_CENTER_ID" as call_center_id,
        "INDIVIDUAL_ID" as individual_id,
        "NVMCONTACT_WORLD_NVM_AGENT_ID_C" as nvmcontact_world_nvm_agent_id,
        "JIRA_ID_C" as jira_id,
        "NAME" as name,
        cast("LAST_LOGIN_DATE" as timestamp) as last_login_date,
        cast("CREATED_DATE" as timestamp) as created_date,
        cast("LAST_MODIFIED_DATE" as timestamp) as last_modified_date,
        cast("SYSTEM_MODSTAMP" as timestamp) as system_modstamp,
        cast("PASSWORD_EXPIRATION_DATE" as timestamp)
            as password_expiration_date,
        cast("SU_ACCESS_EXPIRATION_DATE" as date) as su_access_expiration_date,
        cast("OFFLINE_TRIAL_EXPIRATION_DATE" as timestamp)
            as offline_trial_expiration_date,
        cast("OFFLINE_PDA_TRIAL_EXPIRATION_DATE" as timestamp)
            as offline_pda_trial_expiration_date,
        cast("LAST_VIEWED_DATE" as timestamp) as last_viewed_date,
        cast("LAST_REFERENCED_DATE" as timestamp) as last_referenced_date,
        cast(
            "NVMCONTACT_WORLD_MOST_RECENT_CALL_EVENT_TIMESTAMP_C" as timestamp
        ) as nvmcontact_world_most_recent_call_event_timestamp,
        cast("DFSLE_PROVISIONED_C" as date) as dfsle_provisioned,
        cast("AWAY_END_DATE_TIME_C" as timestamp) as away_end_date_time,
        cast("AWAY_START_DATE_TIME_C" as timestamp) as away_start_date_time,
        cast("_FIVETRAN_SYNCED" as timestamp) as fivetran_synced,
        "USERNAME" as username,
        "LAST_NAME" as last_name,
        "FIRST_NAME" as first_name,
        "COMPANY_NAME" as company_name,
        "DIVISION" as division,
        "DEPARTMENT" as department,
        "TITLE" as title,
        "STREET" as street,
        "CITY" as city,
        "STATE" as state,
        "POSTAL_CODE" as postal_code,
        "COUNTRY" as country,
        "LATITUDE" as latitude,
        "LONGITUDE" as longitude,
        "GEOCODE_ACCURACY" as geocode_accuracy,
        "EMAIL" as email,
        "EMAIL_PREFERENCES_AUTO_BCC" as email_preferences_auto_bcc,
        "EMAIL_PREFERENCES_AUTO_BCC_STAY_IN_TOUCH"
            as email_preferences_auto_bcc_stay_in_touch,
        "EMAIL_PREFERENCES_STAY_IN_TOUCH_REMINDER"
            as email_preferences_stay_in_touch_reminder,
        "SENDER_EMAIL" as sender_email,
        "SENDER_NAME" as sender_name,
        "SIGNATURE" as signature,
        "STAY_IN_TOUCH_SUBJECT" as stay_in_touch_subject,
        "STAY_IN_TOUCH_SIGNATURE" as stay_in_touch_signature,
        "STAY_IN_TOUCH_NOTE" as stay_in_touch_note,
        "PHONE" as phone,
        "FAX" as fax,
        "MOBILE_PHONE" as mobile_phone,
        "ALIAS" as alias,
        "COMMUNITY_NICKNAME" as community_nickname,
        "BADGE_TEXT" as badge_text,
        "IS_ACTIVE" as is_active,
        "TIME_ZONE_SID_KEY" as time_zone_sid_key,
        "LOCALE_SID_KEY" as locale_sid_key,
        "RECEIVES_INFO_EMAILS" as receives_info_emails,
        "RECEIVES_ADMIN_INFO_EMAILS" as receives_admin_info_emails,
        "EMAIL_ENCODING_KEY" as email_encoding_key,
        "USER_TYPE" as user_type,
        "START_DAY" as start_day,
        "END_DAY" as end_day,
        "LANGUAGE_LOCALE_KEY" as language_locale_key,
        "EMPLOYEE_NUMBER" as employee_number,
        "USER_PERMISSIONS_MARKETING_USER" as user_permissions_marketing_user,
        "USER_PERMISSIONS_OFFLINE_USER" as user_permissions_offline_user,
        "USER_PERMISSIONS_AVANTGO_USER" as user_permissions_avantgo_user,
        "USER_PERMISSIONS_CALL_CENTER_AUTO_LOGIN"
            as user_permissions_call_center_auto_login,
        "USER_PERMISSIONS_SFCONTENT_USER" as user_permissions_sfcontent_user,
        "USER_PERMISSIONS_KNOWLEDGE_USER" as user_permissions_knowledge_user,
        "USER_PERMISSIONS_INTERACTION_USER"
            as user_permissions_interaction_user,
        "USER_PERMISSIONS_SUPPORT_USER" as user_permissions_support_user,
        "USER_PERMISSIONS_CHATTER_ANSWERS_USER"
            as user_permissions_chatter_answers_user,
        "FORECAST_ENABLED" as forecast_enabled,
        "USER_PREFERENCES_ACTIVITY_REMINDERS_POPUP"
            as user_preferences_activity_reminders_popup,
        "USER_PREFERENCES_EVENT_REMINDERS_CHECKBOX_DEFAULT"
            as user_preferences_event_reminders_checkbox_default,
        "USER_PREFERENCES_TASK_REMINDERS_CHECKBOX_DEFAULT"
            as user_preferences_task_reminders_checkbox_default,
        "USER_PREFERENCES_REMINDER_SOUND_OFF"
            as user_preferences_reminder_sound_off,
        "USER_PREFERENCES_DISABLE_ALL_FEEDS_EMAIL"
            as user_preferences_disable_all_feeds_email,
        "USER_PREFERENCES_DISABLE_FOLLOWERS_EMAIL"
            as user_preferences_disable_followers_email,
        "USER_PREFERENCES_DISABLE_PROFILE_POST_EMAIL"
            as user_preferences_disable_profile_post_email,
        "USER_PREFERENCES_DISABLE_CHANGE_COMMENT_EMAIL"
            as user_preferences_disable_change_comment_email,
        "USER_PREFERENCES_DISABLE_LATER_COMMENT_EMAIL"
            as user_preferences_disable_later_comment_email,
        "USER_PREFERENCES_DIS_PROF_POST_COMMENT_EMAIL"
            as user_preferences_dis_prof_post_comment_email,
        "USER_PREFERENCES_CONTENT_NO_EMAIL"
            as user_preferences_content_no_email,
        "USER_PREFERENCES_CONTENT_EMAIL_AS_AND_WHEN"
            as user_preferences_content_email_as_and_when,
        "USER_PREFERENCES_APEX_PAGES_DEVELOPER_MODE"
            as user_preferences_apex_pages_developer_mode,
        "USER_PREFERENCES_RECEIVE_NO_NOTIFICATIONS_AS_APPROVER"
            as user_preferences_receive_no_notifications_as_approver,
        "USER_PREFERENCES_RECEIVE_NOTIFICATIONS_AS_DELEGATED_APPROVER"
            as user_preferences_receive_notifications_as_delegated_approver,
        "USER_PREFERENCES_HIDE_CSNGET_CHATTER_MOBILE_TASK"
            as user_preferences_hide_csnget_chatter_mobile_task,
        "USER_PREFERENCES_DISABLE_MENTIONS_POST_EMAIL"
            as user_preferences_disable_mentions_post_email,
        "USER_PREFERENCES_DIS_MENTIONS_COMMENT_EMAIL"
            as user_preferences_dis_mentions_comment_email,
        "USER_PREFERENCES_HIDE_CSNDESKTOP_TASK"
            as user_preferences_hide_csndesktop_task,
        "USER_PREFERENCES_HIDE_CHATTER_ONBOARDING_SPLASH"
            as user_preferences_hide_chatter_onboarding_splash,
        "USER_PREFERENCES_HIDE_SECOND_CHATTER_ONBOARDING_SPLASH"
            as user_preferences_hide_second_chatter_onboarding_splash,
        "USER_PREFERENCES_DIS_COMMENT_AFTER_LIKE_EMAIL"
            as user_preferences_dis_comment_after_like_email,
        "USER_PREFERENCES_DISABLE_LIKE_EMAIL"
            as user_preferences_disable_like_email,
        "USER_PREFERENCES_SORT_FEED_BY_COMMENT"
            as user_preferences_sort_feed_by_comment,
        "USER_PREFERENCES_DISABLE_MESSAGE_EMAIL"
            as user_preferences_disable_message_email,
        "USER_PREFERENCES_DISABLE_BOOKMARK_EMAIL"
            as user_preferences_disable_bookmark_email,
        "USER_PREFERENCES_ALLOW_CONVERSATION_REMINDERS"
            as user_preferences_allow_conversation_reminders,
        "USER_PREFERENCES_DISABLE_SHARE_POST_EMAIL"
            as user_preferences_disable_share_post_email,
        "USER_PREFERENCES_ACTION_LAUNCHER_EINSTEIN_GPT_CONSENT"
            as user_preferences_action_launcher_einstein_gpt_consent,
        "USER_PREFERENCES_ASSISTIVE_ACTIONS_ENABLED_IN_ACTION_LAUNCHER"
            as user_preferences_assistive_actions_enabled_in_action_launcher,
        "USER_PREFERENCES_ENABLE_AUTO_SUB_FOR_FEEDS"
            as user_preferences_enable_auto_sub_for_feeds,
        "USER_PREFERENCES_DISABLE_FILE_SHARE_NOTIFICATIONS_FOR_API"
            as user_preferences_disable_file_share_notifications_for_api,
        "USER_PREFERENCES_SHOW_TITLE_TO_EXTERNAL_USERS"
            as user_preferences_show_title_to_external_users,
        "USER_PREFERENCES_SHOW_MANAGER_TO_EXTERNAL_USERS"
            as user_preferences_show_manager_to_external_users,
        "USER_PREFERENCES_SHOW_EMAIL_TO_EXTERNAL_USERS"
            as user_preferences_show_email_to_external_users,
        "USER_PREFERENCES_SHOW_WORK_PHONE_TO_EXTERNAL_USERS"
            as user_preferences_show_work_phone_to_external_users,
        "USER_PREFERENCES_SHOW_MOBILE_PHONE_TO_EXTERNAL_USERS"
            as user_preferences_show_mobile_phone_to_external_users,
        "USER_PREFERENCES_SHOW_FAX_TO_EXTERNAL_USERS"
            as user_preferences_show_fax_to_external_users,
        "USER_PREFERENCES_SHOW_STREET_ADDRESS_TO_EXTERNAL_USERS"
            as user_preferences_show_street_address_to_external_users,
        "USER_PREFERENCES_SHOW_CITY_TO_EXTERNAL_USERS"
            as user_preferences_show_city_to_external_users,
        "USER_PREFERENCES_SHOW_STATE_TO_EXTERNAL_USERS"
            as user_preferences_show_state_to_external_users,
        "USER_PREFERENCES_SHOW_POSTAL_CODE_TO_EXTERNAL_USERS"
            as user_preferences_show_postal_code_to_external_users,
        "USER_PREFERENCES_SHOW_COUNTRY_TO_EXTERNAL_USERS"
            as user_preferences_show_country_to_external_users,
        "USER_PREFERENCES_SHOW_PROFILE_PIC_TO_GUEST_USERS"
            as user_preferences_show_profile_pic_to_guest_users,
        "USER_PREFERENCES_SHOW_TITLE_TO_GUEST_USERS"
            as user_preferences_show_title_to_guest_users,
        "USER_PREFERENCES_SHOW_CITY_TO_GUEST_USERS"
            as user_preferences_show_city_to_guest_users,
        "USER_PREFERENCES_SHOW_STATE_TO_GUEST_USERS"
            as user_preferences_show_state_to_guest_users,
        "USER_PREFERENCES_SHOW_POSTAL_CODE_TO_GUEST_USERS"
            as user_preferences_show_postal_code_to_guest_users,
        "USER_PREFERENCES_SHOW_COUNTRY_TO_GUEST_USERS"
            as user_preferences_show_country_to_guest_users,
        "USER_PREFERENCES_SHOW_FORECASTING_CHANGE_SIGNALS"
            as user_preferences_show_forecasting_change_signals,
        "USER_PREFERENCES_LIVE_AGENT_MIAW_SETUP_DEFLECTION"
            as user_preferences_live_agent_miaw_setup_deflection,
        "USER_PREFERENCES_HIDE_S_1_BROWSER_UI"
            as user_preferences_hide_s_1_browser_ui,
        "USER_PREFERENCES_DISABLE_ENDORSEMENT_EMAIL"
            as user_preferences_disable_endorsement_email,
        "USER_PREFERENCES_PATH_ASSISTANT_COLLAPSED"
            as user_preferences_path_assistant_collapsed,
        "USER_PREFERENCES_CACHE_DIAGNOSTICS"
            as user_preferences_cache_diagnostics,
        "USER_PREFERENCES_SHOW_EMAIL_TO_GUEST_USERS"
            as user_preferences_show_email_to_guest_users,
        "USER_PREFERENCES_SHOW_MANAGER_TO_GUEST_USERS"
            as user_preferences_show_manager_to_guest_users,
        "USER_PREFERENCES_SHOW_WORK_PHONE_TO_GUEST_USERS"
            as user_preferences_show_work_phone_to_guest_users,
        "USER_PREFERENCES_SHOW_MOBILE_PHONE_TO_GUEST_USERS"
            as user_preferences_show_mobile_phone_to_guest_users,
        "USER_PREFERENCES_SHOW_FAX_TO_GUEST_USERS"
            as user_preferences_show_fax_to_guest_users,
        "USER_PREFERENCES_SHOW_STREET_ADDRESS_TO_GUEST_USERS"
            as user_preferences_show_street_address_to_guest_users,
        "USER_PREFERENCES_LIGHTNING_EXPERIENCE_PREFERRED"
            as user_preferences_lightning_experience_preferred,
        "USER_PREFERENCES_HIDE_END_USER_ONBOARDING_ASSISTANT_MODAL"
            as user_preferences_hide_end_user_onboarding_assistant_modal,
        "USER_PREFERENCES_HIDE_LIGHTNING_MIGRATION_MODAL"
            as user_preferences_hide_lightning_migration_modal,
        "USER_PREFERENCES_HIDE_SFX_WELCOME_MAT"
            as user_preferences_hide_sfx_welcome_mat,
        "USER_PREFERENCES_HIDE_BIGGER_PHOTO_CALLOUT"
            as user_preferences_hide_bigger_photo_callout,
        "USER_PREFERENCES_GLOBAL_NAV_BAR_WTSHOWN"
            as user_preferences_global_nav_bar_wtshown,
        "USER_PREFERENCES_GLOBAL_NAV_GRID_MENU_WTSHOWN"
            as user_preferences_global_nav_grid_menu_wtshown,
        "USER_PREFERENCES_CREATE_LEXAPPS_WTSHOWN"
            as user_preferences_create_lexapps_wtshown,
        "USER_PREFERENCES_FAVORITES_WTSHOWN"
            as user_preferences_favorites_wtshown,
        "USER_PREFERENCES_RECORD_HOME_SECTION_COLLAPSE_WTSHOWN"
            as user_preferences_record_home_section_collapse_wtshown,
        "USER_PREFERENCES_RECORD_HOME_RESERVED_WTSHOWN"
            as user_preferences_record_home_reserved_wtshown,
        "USER_PREFERENCES_FAVORITES_SHOW_TOP_FAVORITES"
            as user_preferences_favorites_show_top_favorites,
        "USER_PREFERENCES_EXCLUDE_MAIL_APP_ATTACHMENTS"
            as user_preferences_exclude_mail_app_attachments,
        "USER_PREFERENCES_SUPPRESS_TASK_SFXREMINDERS"
            as user_preferences_suppress_task_sfxreminders,
        "USER_PREFERENCES_SUPPRESS_EVENT_SFXREMINDERS"
            as user_preferences_suppress_event_sfxreminders,
        "USER_PREFERENCES_PREVIEW_CUSTOM_THEME"
            as user_preferences_preview_custom_theme,
        "USER_PREFERENCES_HAS_CELEBRATION_BADGE"
            as user_preferences_has_celebration_badge,
        "USER_PREFERENCES_USER_DEBUG_MODE_PREF"
            as user_preferences_user_debug_mode_pref,
        "USER_PREFERENCES_SRHOVERRIDE_ACTIVITIES"
            as user_preferences_srhoverride_activities,
        "USER_PREFERENCES_NEW_LIGHTNING_REPORT_RUN_PAGE_ENABLED"
            as user_preferences_new_lightning_report_run_page_enabled,
        "USER_PREFERENCES_REVERSE_OPEN_ACTIVITIES_VIEW"
            as user_preferences_reverse_open_activities_view,
        "USER_PREFERENCES_EMAIL_SUMMARIZATION_GPT_CONSENT"
            as user_preferences_email_summarization_gpt_consent,
        "USER_PREFERENCES_HAS_SENT_WARNING_EMAIL"
            as user_preferences_has_sent_warning_email,
        "USER_PREFERENCES_HAS_SENT_WARNING_EMAIL_238"
            as user_preferences_has_sent_warning_email_238,
        "USER_PREFERENCES_HAS_SENT_WARNING_EMAIL_240"
            as user_preferences_has_sent_warning_email_240,
        "USER_PREFERENCES_NATIVE_EMAIL_CLIENT"
            as user_preferences_native_email_client,
        "USER_PREFERENCES_SEND_LIST_EMAIL_THROUGH_EXTERNAL_SERVICE"
            as user_preferences_send_list_email_through_external_service,
        "USER_PREFERENCES_HIDE_BROWSE_PRODUCT_REDIRECT_CONFIRMATION"
            as user_preferences_hide_browse_product_redirect_confirmation,
        "USER_PREFERENCES_HIDE_ONLINE_SALES_APP_TAB_VISIBILITY_REQUIREMENTS_MODAL"
            as user_preferences_hide_online_sales_app_tab_visibility_requirements_modal,
        "USER_PREFERENCES_HIDE_ONLINE_SALES_APP_WELCOME_MAT"
            as user_preferences_hide_online_sales_app_welcome_mat,
        "USER_PREFERENCES_SHOW_FORECASTING_ROUNDED_AMOUNTS"
            as user_preferences_show_forecasting_rounded_amounts,
        "IS_PARTNER" as is_partner,
        "EXTENSION" as extension,
        "PORTAL_ROLE" as portal_role,
        "IS_PORTAL_ENABLED" as is_portal_enabled,
        "FEDERATION_IDENTIFIER" as federation_identifier,
        "ABOUT_ME" as about_me,
        "FULL_PHOTO_URL" as full_photo_url,
        "SMALL_PHOTO_URL" as small_photo_url,
        "IS_EXT_INDICATOR_VISIBLE" as is_ext_indicator_visible,
        "OUT_OF_OFFICE_MESSAGE" as out_of_office_message,
        "MEDIUM_PHOTO_URL" as medium_photo_url,
        "DIGEST_FREQUENCY" as digest_frequency,
        "DEFAULT_GROUP_NOTIFICATION_FREQUENCY"
            as default_group_notification_frequency,
        "BANNER_PHOTO_URL" as banner_photo_url,
        "SMALL_BANNER_PHOTO_URL" as small_banner_photo_url,
        "MEDIUM_BANNER_PHOTO_URL" as medium_banner_photo_url,
        "IS_PROFILE_PHOTO_ACTIVE" as is_profile_photo_active,
        "DSFS_DSPRO_SFMEMBERSHIP_STATUS_C" as dsfs_dspro_sfmembership_status,
        "DSFS_DSPRO_SFPASSWORD_C" as dsfs_dspro_sfpassword,
        "DSFS_DSPRO_SFUSERNAME_C" as dsfs_dspro_sfusername,
        "NVMCONTACT_WORLD_MOST_RECENT_CALL_IS_ACTIVE_C"
            as nvmcontact_world_most_recent_call_is_active,
        "NVMCONTACT_WORLD_MOST_RECENT_CALL_C"
            as nvmcontact_world_most_recent_call,
        "NVMCONTACT_WORLD_NOTES_COLLAPSED_C"
            as nvmcontact_world_notes_collapsed,
        "ET_4_AE_5_DEFAULT_ET_PAGE_C" as et_4_ae_5_default_et_page,
        "ET_4_AE_5_DEFAULT_MID_C" as et_4_ae_5_default_mid,
        "ET_4_AE_5_EXACT_TARGET_FOR_APP_EXCHANGE_ADMIN_C"
            as et_4_ae_5_exact_target_for_app_exchange_admin,
        "ET_4_AE_5_EXACT_TARGET_FOR_APP_EXCHANGE_USER_C"
            as et_4_ae_5_exact_target_for_app_exchange_user,
        "ET_4_AE_5_EXACT_TARGET_USERNAME_C" as et_4_ae_5_exact_target_username,
        "ET_4_AE_5_EXACT_TARGET_OAUTH_TOKEN_C"
            as et_4_ae_5_exact_target_oauth_token,
        "ET_4_AE_5_VALID_EXACT_TARGET_ADMIN_C"
            as et_4_ae_5_valid_exact_target_admin,
        "ET_4_AE_5_VALID_EXACT_TARGET_USER_C"
            as et_4_ae_5_valid_exact_target_user,
        "WBSENDIT_CAMPAIGN_MONITOR_USER_C" as wbsendit_campaign_monitor_user,
        "SPLIT_THE_BILLS_SIGNATURE_C" as split_the_bills_signature,
        "UNIHOMES_SIGNATURE_C" as unihomes_signature,
        "DFSLE_CAN_MANAGE_ACCOUNT_C" as dfsle_can_manage_account,
        "DFSLE_STATUS_C" as dfsle_status,
        "DFSLE_USERNAME_C" as dfsle_username,
        "AVATAR_URL_C" as avatar_url,
        "DELEGATED_PERSON_C" as delegated_person,
        "_FIVETRAN_DELETED" as fivetran_deleted,
        "HAS_USER_VERIFIED_EMAIL" as has_user_verified_email,
        "HAS_USER_VERIFIED_PHONE" as has_user_verified_phone
    from source

)

select *
from snake_case_field_names_and_clean_timestamps
