{% set years = ['2021-2022', '2022-2023', '2023-2024', '2024-2025', '2025-2026'] %}
{% set metrics = [
    'total_order_forms',
    'target_this_season',
    'live_properties_in_season',
    'current_live_properties',
    'total_leads',
    'total_enquiries',
    'unique_featured_properties_purchased'
] %}

WITH agent_accounts AS (

    SELECT
        account_id,
        name AS account_name,
        record_type_name,
        associated_city,
        billing_postal_code,
        date_closed,
        date_won,
        total_student_portfolio,
        account_type
    FROM {{ ref('stg_salesforce__accounts') }}
    WHERE record_type_name = 'Landlord/Agent'

),

{% for year in years %}
analytics_{{ year | replace('-', '_') }} AS (
    SELECT
        landlord AS account_id,
        {% for metric in metrics %}
            {{ metric }} AS {{ metric }}_{{ year[:4] }}_{{ year[5:] }}{% if not loop.last %},{% endif %}
        {% endfor %}
    FROM {{ ref('stg_salesforce__analytics') }}
    WHERE name = '{{ year }}'
){% if not loop.last %},{% endif %}
{% endfor %},

all_analytics_accounts AS (
    SELECT DISTINCT landlord AS account_id
    FROM {{ ref('stg_salesforce__analytics') }}
),

analytics_pivoted AS (

    SELECT
        ids.account_id,

        {% for year in years %}
            {% for metric in metrics %}
                COALESCE(
                    a_{{ year | replace('-', '_') }}.{{ metric }}_{{ year[:4] }}_{{ year[5:] }},
                    0
                ) AS {{ metric }}_{{ year[:4] }}_{{ year[5:] }}{% if not loop.last or not year == years[-1] %},{% endif %}
            {% endfor %}
        {% endfor %}

    FROM all_analytics_accounts AS ids

    {% for year in years %}
    LEFT JOIN analytics_{{ year | replace('-', '_') }} AS a_{{ year | replace('-', '_') }}
        ON ids.account_id = a_{{ year | replace('-', '_') }}.account_id
    {% endfor %}
)

SELECT
    acc.*,

    {% for year in years %}
        {% for metric in metrics %}
            analytics.{{ metric }}_{{ year[:4] }}_{{ year[5:] }}{% if not loop.last or not year == years[-1] %},{% endif %}
        {% endfor %}
    {% endfor %}

FROM agent_accounts AS acc
LEFT JOIN analytics_pivoted AS analytics
    ON acc.account_id = analytics.account_id
