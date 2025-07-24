with

-- Staging opportunity records
opportunities as (
    select *
    from {{ ref('stg_salesforce__opportunities') }}
),

-- Staging opportunity products
opportunity_products as (
    select *
    from {{ ref('stg_salesforce__opportunity_products') }}
),

-- Aggregated portfolio info (inlined logic)
opportunity_portfolio_aggregation as (
    select
        opportunity_id,

        -- Portfolio size for each type
        sum(case when lower(portfolio_type) = 'student' then quantity_0 else 0 end) as student_portfolio_size,
        sum(case when lower(portfolio_type) = 'sharer' then quantity_0 else 0 end) as sharer_portfolio_size,

        -- Total portfolio size (across all types)
        sum(quantity_0) as total_portfolio_size

    from opportunity_products
    group by opportunity_id
),

-- Staging Account Info
accounts as (
    select
        account_id,
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

        -- Account info (exclude duplicate account_id)
        acc.account_owner_id,
        acc.parent_account_id,
        acc.landlord_agent_name,
        acc.annual_revenue,
        acc.account_type,
        acc.account_record_type_name,
        acc.account_site,
        acc.industry,
        acc.rating,
        acc.sic,
        acc.ticker_symbol,
        acc.number_of_employees,
        acc.ownership,
        acc.account_number,
        acc.account_last_activity_date,
        acc.account_created_date,
        acc.account_last_modified_date,
        acc.account_description

    from opportunities opp
    left join opportunity_portfolio_aggregation opa
        on opp.opportunity_id = opa.opportunity_id

    left join accounts acc
        on opp.account_id = acc.account_id
)

select *
from joined
