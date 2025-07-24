with opportunities as (
    select *
    from {{ source('salesforce', 'opportunity') }}
),

accounts as (
    select id as account_id, name as account_name
    from {{ source('salesforce', 'account') }}
),

users as (
    select id as user_id, name as user_name
    from {{ source('salesforce', 'user') }}
),

record_types as (
    select id as record_type_id, name as record_type_name
    from {{ source('salesforce', 'record_type') }}
)
-----------------
-- ChatGPT auto-generated SQL to get names for foreign keys
-- Commented out tables not currently required/ FiveTran Synced
-- JINJA double brackets removed to not affect model run
----------------
/*,

contacts as (
    select id as contact_id, name as contact_name
    from  source('salesforce', 'contact') 
),

campaigns as (
    select id as campaign_id, name as campaign_name
    from  source('salesforce', 'campaign') 
),

pricebooks as (
    select id as pricebook_2_id, name as pricebook_name
    from  source('salesforce', 'pricebook2') 
),

quotes as (
    select id as synced_quote_id, name as synced_quote_name
    from  source('salesforce', 'quote') 
)*/

select
    opp.*,

    -- Foreign key name lookups
    acct.account_name,
    rt.record_type_name,
    owner.user_name as owner_name,
    created_by.user_name as created_by_name,
    modified_by.user_name as last_modified_by_name/*
    con.contact_name,
    camp.campaign_name,
    pb.pricebook_name,
    quote.synced_quote_name */

from opportunities opp
left join accounts acct
    on opp.account_id = acct.account_id
left join record_types rt
    on opp.record_type_id = rt.record_type_id
left join users owner
    on opp.owner_id = owner.user_id
left join users created_by
    on opp.created_by_id = created_by.user_id
left join users modified_by
    on opp.last_modified_by_id = modified_by.user_id
/*left join contacts con
    on opp.contact_id = con.contact_id
left join campaigns camp
    on opp.campaign_id = camp.campaign_id
left join pricebooks pb
    on opp.pricebook_2_id = pb.pricebook_2_id
left join quotes quote
    on opp.synced_quote_id = quote.synced_quote_id */

