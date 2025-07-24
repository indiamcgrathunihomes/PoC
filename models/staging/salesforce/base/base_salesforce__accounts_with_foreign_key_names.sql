with accounts as (
    select *
    from {{ source('salesforce', 'account') }}
),

record_types as (
    select id as record_type_id, name as record_type_name
    from {{ source('salesforce', 'record_type') }}
),

users as (
    select id as user_id, name
    from {{ source('salesforce', 'user') }}
),

parent_accounts as (
    select id as account_id, name as parent_account_name
    from {{ source('salesforce', 'account') }}
),

master_accounts as (
    select id as master_record_id, name as master_record_name
    from {{ source('salesforce', 'account') }}
)

select
    acc.*,

    -- Replacing IDs with names
    rt.record_type_name,
    owner.name as owner_name,
    created_by.name as created_by_name,
    last_modified_by.name as last_modified_by_name,
    parent.parent_account_name

from accounts acc
left join record_types rt
    on acc.record_type_id = rt.record_type_id
left join users owner
    on acc.owner_id = owner.user_id
left join users created_by
    on acc.created_by_id = created_by.user_id
left join users last_modified_by
    on acc.last_modified_by_id = last_modified_by.user_id
left join parent_accounts parent
    on acc.parent_id = parent.account_id
