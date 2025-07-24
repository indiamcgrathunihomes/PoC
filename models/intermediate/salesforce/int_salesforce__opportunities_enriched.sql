with

-- Base opportunity records
opportunities as (
    select *
    from {{ ref('stg_salesforce__opportunities') }}
),

-- Aggregated portfolio info from opportunity products
opportunity_products_aggregated_to_opportunity as (
    select *
    from {{ ref('int_salesforce__opportunity_products_aggregated_to_opportunity') }}
),

-- Accounts
accounts as (
    select
        account_id,
        name as account_name,
        owner_id as account_owner_id,
        parent_id as parent_account_id,
        landlord_agent_name,
        annual_revenue,
        type as account_type,
        record_type_name as account_record_type_name,
        site as account_site,
        industry,
        rating,
        sic,
        ticker_symbol,
        number_of_employees,
        ownership,
        account_number,
        last_activity_date as account_last_activity_date,
        created_date as account_created_date,
        last_modified_date as account_last_modified_date,
        description as account_description
    from {{ ref('stg_salesforce__accounts') }}
),

-- Final join
joined as (
    select
        opp.*,

        -- Portfolio info
        opa.student_portfolio_size,
        opa.sharer_portfolio_size,
        opa.total_portfolio_size,

        -- Account info
        acc.*

    from opportunities opp
    left join opportunity_products_aggregated_to_opportunity opa
        on opp.opportunity_id = opa.opportunity_id

    left join accounts acc
        on opp.account_id = acc.account_id
)

select *
from joined
