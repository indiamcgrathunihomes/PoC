-- Rebuilding query for
-- https://unihomes.lightning.force.com/lightning/r/Report/00OUc0000069OinMAE/view
with
client_list_consistent_with_board_kpi_reporting as (
    select
        acc.account_id as salesforce_18_digit_id,
        acc.account_id as record_id,
        acc.account_name as company,
        acc.account_name as name,
        acc.associated_city,
        acc.billing_postal_code as postcode,
        acc.total_student_portfolio,
        acc.account_type,
        'Customer' as category,
        'Account' as sf_object,
        null as phone,
        null as competitor,
        acc.record_type_name as record_type_name,
        null as email,
        null as stage,
        acc.total_order_forms_2025_2026 as total_order_forms
    from {{ ref("int_salesforce__analytics_pivoted_by_account") }} as acc
    where
        1 = 1
        and acc.record_type_name = 'Landlord/Agent'
        and acc.date_closed is null
        and acc.date_won is not null
),

active_new_business_b2b_opportunities as (
    -- Rebuilding query for
    -- https://unihomes.lightning.force.com/lightning/r/Report/00OUc0000069QmDMAU/view
    select
        account_id as salesforce_18_digit_id,
        opportunity_id as record_id,
        account_name as company,
        account_name as name,
        account_associated_city as associated_city,
        account_billing_postal_code as postcode,
        total_student_portfolio,
        account_type,
        'Prospect' as category,
        'Opportunity' as sf_object,
        null as phone,
        competitor_name as competitor,
        record_type_name as record_type,
        null as email,
        null as stage,
        0 as total_order_forms
    from {{ ref("int_salesforce__opportunities_enriched") }}

    where
        1 = 1
        and account_record_type_name = 'Landlord/Agent'
        and record_type_name = 'New Business B2B'
        and account_date_closed is null
        and account_date_won is null
        and stage_name in (
            'Renewal',
            'Fact Find',
            'Meeting Booked',
            'Negotiation',
            'Agreement Preparation',
            'Agreement Sent',
            'Trial'
        )
),

valid_prospect_main_contact_leads as (
    -- Rebuilding query for
    -- https://unihomes.lightning.force.com/lightning/r/Report/00OUc0000069OyvMAE/view
    select
        lead_id as salesforce_18_digit_id,
        lead_id as record_id,
        company,
        name,
        associated_city,
        postal_code as postcode,
        percentage_student as total_student_portfolio,
        null as account_type,
        'Prospect' as category,
        'Lead' as sf_object,
        phone,
        competitor_name as competitor,
        record_type_name as record_type,
        email,
        null as stage,
        0 as total_order_forms
    from {{ ref("stg_salesforce__leads") }}

    where
        1 = 1
        and record_type_name = 'Letting Agent/Landlord'
        and main_contact = true
        and status <> 'Converted'
        and status in (
            'Unqualified this Season',
            'New',
            'Working',
            'Nurture'
        )

),

combined_data_table as (
    select *
    from client_list_consistent_with_board_kpi_reporting
    union
    select *
    from active_new_business_b2b_opportunities
    union
    select *
    from valid_prospect_main_contact_leads
)

select *
from combined_data_table
