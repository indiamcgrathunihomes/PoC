with analytics as (
    select *
    from {{ ref('stg_salesforce__analytics') }}
),

accounts as (
    select
        account_id,
        name
    from {{ ref('stg_salesforce__accounts') }}
),

users as (
    select
        user_id,
        name 
    from {{ ref('stg_salesforce__users') }}
),

analytics_with_foreign_keys as (

    select
        a.*,

        acc.name AS account_name,
        created_by.name as created_by_name,
        last_modified_by.name as last_modified_by_name

    from analytics a

    -- Link to Account (landlord)
    left join accounts acc
        on a.landlord = acc.account_id

    -- Link to Created By user
    left join users created_by
        on a.created_by_id = created_by.user_id

    -- Link to Last Modified By user
    left join users last_modified_by
        on a.last_modified_by_id = last_modified_by.user_id
)

select *
from analytics_with_foreign_keys
