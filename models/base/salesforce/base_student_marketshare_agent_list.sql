-- Rebuilding query for
-- https://unihomes.lightning.force.com/lightning/r/Report/00OUc0000069OinMAE/view
with
    active_customers as (
        select
            acc.account_id as salesforce_18_digit_id,
            acc.account_id as record_id,
            acc.name as company,
            acc.name as name,
            acc.associated_city,
            acc.billing_postal_code as postcode,
            acc.total_student_portfolio,
            acc.account_type,
            'Customer' as category,
            'Account' as sf_object,
            null as phone,
            null as competitor,
            acc.record_type_name,
            null as email,
            null as stage,
            ana.total_order_forms
        from {{ ref("stg_salesforce__accounts") }} acc
        left join
            {{ ref('stg_salesforce__analytics') }} ana
            on acc.account_id = ana.landlord and ana.name = '2025-2026' -- Static filter for the current FY26
        where
            1 = 1
            and acc.record_type_name = 'Landlord/Agent' 
            and acc.date_closed is null
            and acc.date_won is not null
    ),

    active_opps as (
        -- Rebuilding query for
        -- https://unihomes.lightning.force.com/lightning/r/Report/00OUc0000069QmDMAU/view
        select
            acc.account_id as salesforce_18_digit_id,
            opp.opportunity_id as record_id,
            acc.name as company,
            acc.name as name,
            acc.associated_city,
            acc.billing_postal_code as postcode,
            qty.total_student_portfolio,
            acc.account_type,
            'Prospect' as category,
            'Opportunity' as sf_object,
            null as phone,
            opp.competitor_name as competitor,
            opp.record_type_name as record_type,
            null as email,
            null as stage,
            0 as total_order_forms
        from {{ ref("stg_salesforce__opportunities") }} opp
        left join
            {{ ref("stg_salesforce__accounts") }} acc on opp.account_id = acc.account_id
        left join
            (
                select opportunity_id, sum(quantity_0) as total_student_portfolio
                from {{ ref("stg_salesforce__opportunity_products") }}
                where portfolio_type = 'Student'
                group by opportunity_id
            ) qty
            on qty.opportunity_id = opp.opportunity_id
        where
            1 = 1
            and acc.record_type_name = 'Landlord/Agent'
            and acc.date_closed is null
            and acc.date_won is null
            and opp.stage_name in (
                'Renewal',
                'Fact Find',
                'Meeting Booked',
                'Negotiation',
                'Agreement Preparation',
                'Agreement Sent',
                'Trial'
            )
    ),

    valid_leads as (
        -- Rebuilding query for
        -- https://unihomes.lightning.force.com/lightning/r/Report/00OUc0000069OyvMAE/view
        select
            lead_id as salesforce_18_digit_id,
            lead_id as record_id,
            company,
            name as name,
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
        from active_customers
        union
        select *
        from active_opps
        union
        select *
        from valid_leads
    )

select *
from combined_data_table
