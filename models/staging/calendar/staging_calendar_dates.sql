select
    calendar_date as date,
    calendar_lettings_year as lettings_year,
    calendar_tenancy_year as tenancy_year,
    calendar_financial_year as financial_year
from
    {{ source('calendar', 'dates') }}