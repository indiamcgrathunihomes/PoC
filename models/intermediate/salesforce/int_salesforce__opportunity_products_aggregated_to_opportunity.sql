with

opportunity_products as (
    select *
    from {{ ref('stg_salesforce__opportunity_products') }}
),

aggregated as (
    select
        opportunity_id,

        -- Portfolio size for each type
        sum(case when lower(portfolio_type) = 'student' then quantity_0 else 0 end) as student_portfolio_size,
        sum(case when lower(portfolio_type) = 'sharer' then quantity_0 else 0 end) as sharer_portfolio_size,

        -- Total portfolio size (across both types and any others)
        sum(quantity_0) as total_portfolio_size

    from opportunity_products
    group by opportunity_id
)

select *
from aggregated
