{{ 
    config(
        materialized='view', 
        tags=['staging','tiktok']
    ) 
}}

with source as (
    select
        zipcode::varchar    as zipcode,
        latitude::float     as latitude,
        longitude::float     as longitude
    from {{ source('RAW', 'ZIP_LAT_LON') }}
)

select
    zipcode,
    latitude,
    longitude
from source
