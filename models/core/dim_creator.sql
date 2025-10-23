{{
  config(
    materialized = 'table',
    tags = ['dim','tiktok']
  )
}}

-- current handle from handle-to-ID timeline
with current_handle as (
  select
    tiktok_id       as tiktok_id,
    handle                  as current_handle_name
  from {{ ref('dim_creator_handle_log') }} -- from staging table
  where is_current_owner = TRUE
),

--- add in tikok IDs without handles
--- add in handles with no tiktok IDs and a suggogate ID + boolean to identify that we're missing the ID
-- put this in a staging table stg_creator

-- dim_creator_stats
-- ec level -- lives in multiple tables -- pull this in with dates as a log and then calc most recent --> this is what goes into dim_creator
----- tiktok_id, stat_name, stat_value, updated_at, is_current -- for each stat and then union
-- dim_creator_email 
-- email -- lives in multiple tables -- pull this in with dates as a log and source info


--- THEN CREATE dim_creator which pulls from stg_creator and 


-- create a stats table that combines stats across all tables 

-- latest creatorinfo per tiktok_id
creatorinfo_latest as (
  select *
  from (
    select
      tiktok_id,
      handle_name,
      display_name,
      email_address,
      mcn_name,
      ec_level,
      creator_ec_access_level,
      create_date,
      last_update,
      row_number() over (
        partition by tiktok_id order by last_update desc nulls last
      ) as rn
    from {{ ref('stg_creatorinfo') }}
  )
  where rn = 1
),

-- latest hydrate record
hydrate_latest as (
  select *
  from (
    select
      tiktok_id,
      handle_name,
      email_address,
      language,
      sec_id,
      is_commerce,
      acct_create,
      last_update,
      row_number() over (
        partition by tiktok_id order by last_update desc nulls last
      ) as rn
    from {{ ref('stg_hydrate') }}
  )
  where rn = 1
),

-- latest market record
market_latest as (
  select *
  from (
    select
      tiktok_id,
      handle_name,
      display_name,
      email_address,
      selectionRegion,
      followerCount,
      last_update,
      row_number() over (
        partition by tiktok_id order by last_update desc nulls last
      ) as rn
    from {{ ref('stg_market') }}
  )
  where rn = 1
),

-- latest echo record
echo_latest as (
  select *
  from (
    select
      tiktok_id,
      handle_name,
      gender,
      language,
      email,
      last_updated_time,
      row_number() over (
        partition by tiktok_id order by last_updated_time desc nulls last
      ) as rn
    from {{ ref('stg_echo') }}
  )
  where rn = 1
),

-- unify all ids we know about
all_ids as (
  select tiktok_id from {{ ref('stg_creatorinfo') }}
  union
  select tiktok_id from {{ ref('stg_hydrate') }}
  union
  select tiktok_id from {{ ref('stg_market') }}
  union
  select tiktok_id from {{ ref('stg_echo') }}
)

-- final 1-row-per-creator dimension
select
  ids.tiktok_id,

  ch.current_handle_name,

  coalesce(ci.display_name, mk.display_name) as display_name,
  coalesce(ci.email_address, hy.email_address, ec.email) as email_address,
  coalesce(ec.language, hy.language) as language,
  coalesce(ec.gender, null) as gender,

  ci.mcn_name,
  ci.ec_level,
  ci.creator_ec_access_level,

  hy.sec_id,
  hy.is_commerce,
  mk.selectionRegion as selection_region,
  mk.followerCount   as follower_count,

  ci.create_date::timestamp_ntz  as create_date,
  greatest(
    ci.last_update::timestamp_ntz,
    hy.last_update::timestamp_ntz,
    mk.last_update::timestamp_ntz,
    ec.last_updated_time::timestamp_ntz
  ) as last_updated_at

from all_ids ids
left join current_handle ch on ch.tiktok_id = ids.tiktok_id
left join creatorinfo_latest ci on ci.tiktok_id = ids.tiktok_id
left join hydrate_latest hy on hy.tiktok_id = ids.tiktok_id
left join market_latest mk on mk.tiktok_id = ids.tiktok_id
left join echo_latest ec on ec.tiktok_id = ids.tiktok_id
