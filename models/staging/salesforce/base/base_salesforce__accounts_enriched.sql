with accounts as (
    select * from {{ ref('stg_salesforce__accounts') }}
),

record_types as (
    select record_type_id, name as record_type_name
    from {{ ref('stg_salesforce__record_types') }}
),

/*users as (
    select user_id, name
    from {% raw %}{{ ref('stg_salesforce__users') }}{% endraw %}
), */

parent_accounts as (
    select account_id, name as parent_account_name
    from {{ ref('stg_salesforce__accounts') }}
),

master_accounts as (
    select account_id, name as master_record_name
    from {{ ref('stg_salesforce__accounts') }}
)

select
    acc.*,

    -- Replacing IDs with names
    rt.record_type_name,
   /*owner.name as owner_name,
    created_by.name as created_by_name,
    last_modified_by.name as last_modified_by_name,*/
    parent.parent_account_name,
    master.master_record_name

from accounts acc
left join record_types rt
    on acc.record_type_id = rt.record_type_id
/*left join users owner
    on acc.owner_id = owner.user_id
left join users created_by
    on acc.created_by_id = created_by.user_id
left join users last_modified_by
    on acc.last_modified_by_id = last_modified_by.user_id */
left join parent_accounts parent
    on acc.parent_id = parent.account_id
left join master_accounts master
    on acc.master_record_id = master.account_id
