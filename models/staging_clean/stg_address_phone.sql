{{ 
    config(
        materialized='view', 
        tags=['staging','tiktok']
    ) 
}}


with source as (

    select
        data_id::number                     as data_id,       
        handle_name::varchar                 as handle_name,
        first_last::varchar                  as first_last,
        phone_number::varchar                as phone_number,
        country::varchar                     as country,
        state::varchar                       as state,
        city::varchar                        as city,
        zipcode::varchar                     as zipcode,
        address_1::varchar                   as address_1,
        address_2::varchar                   as address_2
    from {{ source('RAW', 'ADDRESS_PHONE') }}

)

select
    data_id,
    handle_name,
    first_last,
    phone_number,
    country,
    state,
    city,
    zipcode,
    address_1,
    address_2
from source