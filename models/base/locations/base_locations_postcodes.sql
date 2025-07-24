select 
    postcode,
    latitude,
    longitude 
from {{ ref("stg_locations__postcodes") }}
