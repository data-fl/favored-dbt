{{
  config(
    materialized = 'view',
    tags = ['staging','tiktok']
  )
}}

-- 0) Current (handle, id) ownership from the timeline
with current_pairs as (
  select
      handle_name,
      tiktok_id,
      handle_start as observed_at
  from {{ ref('stg_tiktok_id_handle_map_time') }}
  where is_current_owner = true
),

-- 1) Raw observations with unified timestamp rule
raw_obs as (
  select
      handle_name,
      tiktok_id,
      coalesce(least(created_at, updated_at),
               coalesce(created_at, updated_at)) as observed_at
  from {{ ref('stg_tiktok_id_handle_map') }}
),

-- 2) IDs that have NEVER had a non-blank handle
ids_without_any_handle as (
  select
      tiktok_id,
      min(observed_at) as first_observed_at
  from raw_obs
  where tiktok_id is not null
  group by tiktok_id
  having sum(case when handle_name is not null then 1 else 0 end) = 0
),

-- 3) Handles that have NEVER had a tiktok_id
handles_without_any_id as (
  select
      lower(trim(handle_name)) as handle_name,
      min(observed_at)         as first_observed_at
  from raw_obs
  where handle_name is not null
  group by lower(trim(handle_name))
  having sum(case when tiktok_id is not null then 1 else 0 end) = 0
),

-- 4) Normalize current owners
current_norm as (
  select
      handle_name,
      tiktok_id,
      observed_at as first_observed_at
  from current_pairs
),

-- 5) Union the three streams, mint surrogate for handle-only rows
unioned as (
  -- a) current owners
  select
      handle_name,
      tiktok_id,
      first_observed_at,
      false as is_missing_tiktok_id,
      false as is_missing_handle,
      cast(null as varchar) as surrogate_id
  from current_norm

  union all

  -- b) ids that never had a handle
  select
      cast(null as varchar)  as handle_name,
      i.tiktok_id,
      i.first_observed_at,
      false as is_missing_tiktok_id,
      true  as is_missing_handle,
      cast(null as varchar)  as surrogate_id
  from ids_without_any_handle i

  union all

  -- c) handles that never had an id → surrogate from MD5(handle)
  select
      h.handle_name,
      cast(null as varchar) as tiktok_id,
      h.first_observed_at,
      true  as is_missing_tiktok_id,
      false as is_missing_handle,
      'h_' || to_varchar(md5(h.handle_name)) as surrogate_id
  from handles_without_any_id h
),

-- 6) Final shape + stable creator_key (prefer natural id, else surrogate)
final as (
  select
      coalesce(
        cast(tiktok_id as varchar),
        surrogate_id
      ) as creator_key,
      tiktok_id,
      handle_name as handle,
      is_missing_tiktok_id,
      is_missing_handle,
      surrogate_id,
      first_observed_at
  from unioned
)

select *
from final
order by
  case
    when tiktok_id is not null and handle is not null then 0
    when tiktok_id is not null and handle is null     then 1
    else 2
  end,
  handle,
  tiktok_id
