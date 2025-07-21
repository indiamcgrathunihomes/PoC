select
    postcode,
    latitude,
    longitude 
from {{ source("locations", "uk_postcodes") }}