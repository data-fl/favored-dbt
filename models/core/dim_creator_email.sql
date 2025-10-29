{{ 
    config(
        materialized='table',
        tags=['dim','tiktok'],
        unique_key='creator_email_id'
    ) 
}}

with creatorinfo as (

    select
        tiktok_id                     as tiktok_id,
        email_address                 as email, 
        'CREATORINFO'                 as data_source,
        create_date::timestamp_ntz    as created_at_raw,
        last_update::timestamp_ntz    as updated_at_raw
    from {{ ref('stg_creatorinfo') }}

),

hydrate as (

    select
        tiktok_id                     as tiktok_id,
        email_address                 as email,
        'HYDRATE'                     as data_source,
        create_date::timestamp_ntz    as created_at_raw,
        last_update::timestamp_ntz    as updated_at_raw
    from {{ ref('stg_hydrate') }}

),

market as (

    select
        tiktok_id                     as tiktok_id,
        email_address                 as email,
        'MARKET'                      as data_source,
        create_date::timestamp_ntz    as created_at_raw,
        last_update::timestamp_ntz    as updated_at_raw
    from {{ ref('stg_market') }}

),

-- 1) Union all sources and keep only valid (tiktok_id, email) pairs
base as (

    select * from creatorinfo
    union all
    select * from hydrate
    union all
    select * from market

),

base_clean as (

    select
        tiktok_id,
        email,
        data_source,
        created_at_raw as created_at,
        updated_at_raw as updated_at
    from base
    where tiktok_id is not null
      and email is not null

),

-- 2) Collapse to one row per (tiktok_id, email) with min(created_at), max(updated_at)
pair_agg as (

    select
        tiktok_id,
        email,
        min(created_at) as created_at,
        max(updated_at) as updated_at
    from base_clean
    group by 1,2

),

-- 3) Choose data_source for each (tiktok_id, email) from the row with the latest updated_at
latest_source as (

    select tiktok_id, email, data_source
    from (
        select
            bc.*,
            row_number() over (
                partition by tiktok_id, email
                order by updated_at desc, created_at desc
            ) as rn
        from base_clean bc
    )
    where rn = 1

),

-- 4) Join selected data_source back to the aggregated row
core_email as (

    select
        p.tiktok_id,
        p.email,
        ls.data_source,
        p.created_at,
        p.updated_at
    from pair_agg p
    join latest_source ls
      on p.tiktok_id = ls.tiktok_id
     and p.email     = ls.email

)

-- 5) Final: surrogate key + is_current per tiktok_id
select
    /* Deterministic numeric surrogate key without dbt-utils */
    abs(hash(tiktok_id, email))                            as creator_email_id,
    tiktok_id,
    email,
    data_source,
    created_at,
    updated_at,
    case
        when updated_at = max(updated_at) over (partition by tiktok_id)
            then true
        else false
    end                                               as is_current
from core_email
