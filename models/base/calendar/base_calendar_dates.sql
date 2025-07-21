select
    date,
    lettings_year,
    tenancy_year,
    financial_year
from
    {{ ref('staging_calendar_dates') }}