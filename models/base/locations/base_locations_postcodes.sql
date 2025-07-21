select 
    postcode,
    latitude,
    longitude 
from {{ ref("staging_locations_postcodes") }}
