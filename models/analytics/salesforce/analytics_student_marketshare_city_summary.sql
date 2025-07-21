select
    regexp_substr(postcode, '[A-Z]+') as city_proxy,
    sum(
        case
            when category = 'Customer'
            then 1
            when total_student_portfolio >= 5
            then 1
            else 0
        end
    ) as student_letting_agents,
    sum(case when category = 'Customer' then 1 else 0 end) as clients,
    sum(
        case
            when category = 'Customer'
            then 1
            when total_student_portfolio >= 5
            then total_student_portfolio
            else 0
        end
    ) as student_portfolio,
    sum(
        case when category = 'Customer' then total_student_portfolio else 0 end
    ) as clients_student_portfolio

from {{ ref("base_student_marketshare_agent_list") }}
group by 1
order by student_portfolio desc
