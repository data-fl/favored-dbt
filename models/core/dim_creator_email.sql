{{ 
    config(
        materialized='table',
        tags=['dim','tiktok'],
        unique_key='creator_email_id'
    ) 
}}

with creatorinfo as (

    select
        tiktok_id           as tiktok_id,
        email_address       as email,       
        'CREATORINFO'       as data_source,
        create_date         as created_at_raw,
        last_update         as updated_at_raw
    from {{ ref('stg_creatorinfo') }}

),

hydrate as (

    select
        tiktok_id           as tiktok_id,
        email_address       as email,
        'HYDRATE'           as data_source,
        create_date         as created_at_raw,
        last_update         as updated_at_raw
    from {{ ref('stg_hydrate') }}

),

market as (

    select
        tiktok_id           as tiktok_id,
        email_address       as email,
        'MARKET'            as data_source,
        create_date         as created_at_raw,
        last_update         as updated_at_raw
    from {{ ref('stg_market') }}

),

bravo_v1 as (

    select
        tiktok_id           as tiktok_id,
        email               as email,       
        'BRAVO_V1'          as data_source,
        created_at          as created_at_raw,
        updated_at          as updated_at_raw
    from {{ ref('stg_bravo_v1') }}

),

bravo_v2 as (

    select
        tiktok_id           as tiktok_id,
        email               as email,
        'BRAVO_V2'          as data_source,
        created_at          as created_at_raw,
        updated_at          as updated_at_raw
    from {{ ref('stg_bravo_v2') }}

),

echo as (

    select
        tiktok_id           as tiktok_id,
        email               as email,
        'ECHO'              as data_source,
        created_time        as created_at_raw,
        last_updated_time   as updated_at_raw
    from {{ ref('stg_echo') }}

),

base as (

    select * from creatorinfo
    union all
    select * from hydrate
    union all
    select * from market
    union all
    select * from bravo_v1
    union all
    select * from bravo_v2
    union all
    select * from echo

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

pair_agg as (

    select
        tiktok_id,
        email,
        min(created_at) as created_at,
        max(updated_at) as updated_at
    from base_clean
    group by 1,2

),

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

core as (

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

select
    -- Positive, deterministic, portable ID (no dbt-utils)
    to_varchar(abs(hash(tiktok_id, email)))           as creator_email_id,
    tiktok_id,
    email,
    data_source,
    created_at,
    updated_at,
    case when updated_at = max(updated_at) over (partition by tiktok_id)
         then true else false end                     as is_current
from core
