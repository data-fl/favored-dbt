{{
  config(
    materialized = 'view',
    tags = ['staging', 'tiktok']
  )
}}

with

base_creatorinfo as (

  select
    cast(tiktok_id as varchar)         as tiktok_id,
    lower(trim(handle_name))           as handle_name,
    'creatorinfo'                      as source_table,
    cast(create_date as timestamp_ntz) as created_at,
    cast(last_update as timestamp_ntz) as updated_at
  from {{ source('RAW','CREATORINFO') }}

),

base_crmcreator as (

  select
    cast(null as varchar)              as tiktok_id,
    lower(trim(handle_name))           as handle_name,
    'crmcreator'                       as source_table,
    cast(create_date as timestamp_ntz) as created_at,
    cast(last_update as timestamp_ntz) as updated_at
  from {{ source('RAW','CRMCREATOR') }}

),

base_hydrate as (

  select
    cast(tiktok_id as varchar)         as tiktok_id,
    lower(trim(handle_name))           as handle_name,
    'hydrate'                          as source_table,
    cast(acct_create as timestamp_ntz)   as created_at,
    cast(handle_modify as timestamp_ntz) as updated_at
  from {{ source('RAW','HYDRATE') }}

),

base_market as (

  select
    cast(tiktok_id as varchar)         as tiktok_id,
    lower(trim(handle_name))           as handle_name,
    'market'                           as source_table,
    cast(last_update as timestamp_ntz) as updated_at,
    cast(create_date as timestamp_ntz) as created_at
  from {{ source('RAW','MARKET') }}

),

base_searchinfo as (

  select
    cast(null as varchar)              as tiktok_id,
    lower(trim(handle_name))           as handle_name,
    'searchinfo'                       as source_table,
    cast(create_date as timestamp_ntz) as created_at,
    cast(last_update as timestamp_ntz) as updated_at
  from {{ source('RAW','SEARCHINFO') }}

),

/* ---------- DEEP DATASETS: use ONLY tiktok_id; force handle to NULL ---------- */
base_bravo_v1 as (

  select
    cast(tiktok_id as varchar)         as tiktok_id,
    cast(null as varchar)              as handle_name,    -- Ignore handle: DEEP datasets not reliable for handle mapping
    'bravo_v1'                         as source_table,
    cast(created_at as timestamp_ntz)  as created_at,
    cast(updated_at as timestamp_ntz)  as updated_at
  from {{ source('RAW','BRAVO_V1') }}

),

base_bravo_v2 as (

  select
    cast(tiktok_id as varchar)         as tiktok_id,
    cast(null as varchar)              as handle_name,    -- Ignore handle: DEEP datasets not reliable for handle mapping
    'bravo_v2'                         as source_table,
    cast(created_at as timestamp_ntz)  as created_at,
    cast(updated_at as timestamp_ntz)  as updated_at
  from {{ source('RAW','BRAVO_V2') }}

),

base_echo as (

  select
    cast(tiktok_id as varchar)           as tiktok_id,
    cast(null as varchar)                as handle_name,  -- Ignore handle: DEEP datasets not reliable for handle mapping
    'echo'                               as source_table,
    cast(created_time as timestamp_ntz)  as created_at,
    cast(last_updated_time as timestamp_ntz) as updated_at
  from {{ source('RAW','ECHO') }}

),

unioned as (

  select * from base_creatorinfo
  union all select * from base_crmcreator
  union all select * from base_hydrate
  union all select * from base_market
  union all select * from base_searchinfo
  union all select * from base_bravo_v1
  union all select * from base_bravo_v2
  union all select * from base_echo

),

normalized as (

  select
    /* collapse blanks to NULL */
    nullif(trim(tiktok_id), '')          as tiktok_id,
    nullif(lower(trim(handle_name)), '') as handle_name,
    source_table,
    created_at,
    updated_at
  from unioned

)

select *
from normalized
