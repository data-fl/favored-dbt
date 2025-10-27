{{
  config(
    materialized = 'view',
    tags = ['staging', 'tiktok']
  )
}}

with base as (
  select
    handle_name,
    tiktok_id,
    source_table,
    /* Earliest reliable observation of the (handle, tiktok_id) pair */
    coalesce(least(created_at, updated_at),
             coalesce(created_at, updated_at))             as observed_at -- if NULL, then don't use least
  from {{ ref('stg_tiktok_id_handle_map') }}
    where handle_name is not null
    and tiktok_id is not null
),

-- De-noise: use the first time we ever saw (handle, id)
first_seen_pair as (
  select
    handle_name,
    tiktok_id,
    min(observed_at) as handle_start
  from base
  group by 1,2
),

-- Build a handle-level SCD timeline: when ownership changes from one id to the next
handle_timeline as (
  select
    handle_name,
    tiktok_id,
    handle_start,
    lead(handle_start) over (
      partition by handle_name
      order by handle_start, tiktok_id  -- tiebreak to make ordering deterministic
    ) as handle_end
  from first_seen_pair
),

final as (
  select
    handle_name,
    tiktok_id,
    /* For convenience: the first time this handle appeared in our system */
    min(handle_start) over (partition by handle_name)       as created_at,
    handle_start,
    handle_end,
    case when handle_end is null then true else false end    as is_current_owner
  from handle_timeline
)

select *
from final
order by handle_name, handle_start


--The window partitions by handle_name, so the next handle_end is the next time that handle appears with a (possibly different) id.
--Only one row per handle can have handle_end is null → single current owner.
--Practical notes / limitations
--This uses the earliest observation of each (handle, id) pair. If a handle leaves ID A, then months later returns to ID A, simple approach will treat A’s start as the first time A ever had it. 
