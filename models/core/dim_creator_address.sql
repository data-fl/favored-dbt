{{ 
    config(
        materialized='view',
        tags=['dim','tiktok']
    ) 
}}

with ap as (

    select
        lower(nullif(trim(a.handle_name), ''))                    as handle_name,
        a.data_id,
        upper(nullif(trim(a.country), ''))                        as country,        -- uppercased
        upper(nullif(trim(a.state), ''))                          as state,          -- uppercased
        initcap(nullif(trim(a.city), ''))                         as city,           -- first letter each word
        nullif(trim(a.zipcode), '')                               as zipcode_raw,
        lower(nullif(trim(a.address_1), ''))                      as address_1,
        lower(nullif(trim(a.address_2), ''))                      as address_2
    from {{ ref('stg_address_phone') }} a

),

echo as (

    select lower(handle_name) as handle_name, tiktok_id
    from {{ ref('stg_echo') }}
    where tiktok_id is not null

),

zip as (

    select 
        zipcode, 
        latitude, 
        longitude
    from {{ ref('stg_zip_lat_lon') }}

),

joined as (

    select
        e.tiktok_id::varchar                                      as tiktok_id,
        ap.data_id,
        ap.country,
        ap.state,
        ap.city,
        regexp_substr(ap.zipcode_raw, '^[0-9]{5}')                as zipcode,
        ap.address_1,
        ap.address_2,
        'address_phone'                                           as source,
        current_timestamp()                                       as ingested_at
    from ap
    join echo e using (handle_name)
    
),

with_geo as (
    select
        j.*,
        z.latitude,
        z.longitude
    from joined j
    left join zip z
      on z.zipcode = j.zipcode
),

with_ids as (

    select
        /* address_id = hash of normalized address fields (V1, case-insensitive on address lines) */
        hash(lower(country), lower(state), lower(city), lower(zipcode), lower(address_1), lower(address_2)) as address_id,
        tiktok_id,
        country, state, city, zipcode, address_1, address_2,
        latitude, longitude,
        source,
        ingested_at,
        data_id
    from with_geo

),

ranked as (

    /* primary = newest; without source timestamps we use ingested_at, then data_id desc */
    select
        w.*,
        row_number() over (
            partition by tiktok_id
            order by ingested_at desc nulls last, data_id desc, address_id asc
        ) as rn
    from with_ids w
)

select
    tiktok_id,
    address_id,
    country,
    state,
    city,
    zipcode,
    address_1,
    address_2,
    latitude,
    longitude,
    (rn = 1) as is_primary,
    source,
    ingested_at
from ranked
