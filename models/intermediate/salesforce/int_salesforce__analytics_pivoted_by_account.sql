{{ config(materialized = 'table') }}

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

WITH

account_details AS (
    SELECT
        account_id,
        name AS account_name,
        record_type_name,
        associated_city,
        billing_postal_code,
        total_student_portfolio,
        account_type,
        date_closed,
        date_won
    FROM {{ ref('stg_salesforce__accounts') }}
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

all_account_ids AS (
    SELECT DISTINCT landlord AS account_id
    FROM {{ ref('stg_salesforce__analytics') }}
),

pivoted AS (
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

    FROM all_account_ids AS ids

    {% for year in years %}
    LEFT JOIN analytics_{{ year | replace('-', '_') }} AS a_{{ year | replace('-', '_') }}
        ON ids.account_id = a_{{ year | replace('-', '_') }}.account_id
    {% endfor %}
)

SELECT
    p.*,
    a.account_name,
    a.record_type_name,
    a.associated_city,
    a.billing_postal_code,
    a.total_student_portfolio,
    a.account_type,
    a.date_closed,
    a.date_won
FROM pivoted p
LEFT JOIN account_details a
    ON p.account_id = a.account_id
