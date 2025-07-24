select
    calendar_date,
    calendar_lettings_year,
    calendar_tenancy_year,
    calendar_financial_year
from
    {{ ref('stg_calendar__dates') }}