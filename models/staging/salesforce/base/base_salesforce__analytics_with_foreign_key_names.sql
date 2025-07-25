with analytics as (
    select *
    from {{ source('salesforce', 'analytics') }}
),

accounts as (
    select
        id,
        name
    from {{ source('salesforce', 'account') }}
),

users as (
    select
        id,
        name 
    from {{ source('salesforce', 'user') }}
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
        on a.landlord_c = acc.id

    -- Link to Created By user
    left join users created_by
        on a.created_by_id = created_by.id

    -- Link to Last Modified By user
    left join users last_modified_by
        on a.last_modified_by_id = last_modified_by.id
)

select *
from analytics_with_foreign_keys
