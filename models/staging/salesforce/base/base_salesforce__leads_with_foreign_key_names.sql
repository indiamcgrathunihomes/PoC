with leads as (
    select * from {{ source('salesforce', 'lead') }}
),

record_types as (
    select id as record_type_id, name as record_type_name
    from {{ source('salesforce', 'record_type') }}
),

users as (
    select id as user_id, name
    from {{ source('salesforce', 'user') }}
),

accounts as (
    select id as account_id, name as account_name
    from {{ source('salesforce', 'account') }}
),


opportunities as (
    select id as opportunity_id, name as opportunity_name
    from {{ source('salesforce', 'opportunity') }}
),

master_leads as (
    select id as lead_id, name as master_record_name
    from {{ source('salesforce', 'lead') }}
)/*,


contacts as (
    select id as contact_id, name as contact_name
    from  source('salesforce', 'contact') -- JINJA brackets removed for compilation
) */

select
    lead.*,

    -- Replaced names
    master_lead.master_record_name,
    rec_type.record_type_name,
    owner.name as owner_name,
    created_by.name as created_by_name,
    last_modified_by.name as last_modified_by_name,
    converted_account.account_name as converted_account_name,
    converted_opportunity.opportunity_name as converted_opportunity_name --,
    -- converted_contact.contact_name as converted_contact_name

from leads lead
left join master_leads master_lead
    on lead.master_record_id = master_lead.lead_id
left join record_types rec_type
    on lead.record_type_id = rec_type.record_type_id
left join users owner
    on lead.owner_id = owner.user_id
left join users created_by
    on lead.created_by_id = created_by.user_id
left join users last_modified_by
    on lead.last_modified_by_id = last_modified_by.user_id
left join accounts converted_account
    on lead.converted_account_id = converted_account.account_id
left join opportunities converted_opportunity
    on lead.converted_opportunity_id = converted_opportunity.opportunity_id
/*left join contacts converted_contact
    on lead.converted_contact_id = converted_contact.contact_id */
