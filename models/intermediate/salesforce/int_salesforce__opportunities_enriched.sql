with

-- Base opportunity records
opportunities as (
    select *
    from {{ ref('stg_salesforce__opportunities') }}
),

-- Additional info from opportunity products
opportunity_products_aggregated_to_opportunity as (
    select *
    from {{ ref('int_salesforce__opportunity_products_aggregated_to_opportunity') }}
),

-- Join portfolio data to each opportunity
joined as (
    select
        opp.*,

        opa.student_portfolio_size,
        opa.sharer_portfolio_size,
        opa.total_portfolio_size

    from opportunities opp
    left join opportunity_products_aggregated_to_opportunity opa
        on opp.opportunity_id = opa.opportunity_id
)

select *
from joined
