{{ 
    config(
        materialized='incremental',
        incremental_strategy='merge',
        unique_key=['creator_stats_id'],
        tags=['dim','tiktok','stats','log']
    ) 
}}

with base as (

  select
    tiktok_id,
    stat_name,
    stat_value::float as stat_value,
    created_at,
    updated_at,
    source,
    source_priority
  from {{ ref('stg_creator_stats') }}
  where tiktok_id is not null

),

ranked as (

  select
    *,
    -- latest per (creator, stat) by updated_at, then by source priority
    case
      when row_number() over (
        partition by tiktok_id, stat_name
        order by updated_at desc nulls last, source_priority asc nulls last
      ) = 1
      then true else false
    end as is_current
  from base

)

select
  -- immutable log key: includes timestamp + source
  md5(
    coalesce(tiktok_id, '') || '|' ||
    coalesce(stat_name, '') || '|' ||
    coalesce(to_char(updated_at, 'YYYY-MM-DD HH24:MI:SS.FF3'), '') || '|' ||
    coalesce(source, '')
  ) as creator_stats_id,
  tiktok_id,
  stat_name,
  stat_value,
  created_at,
  updated_at,
  is_current,
  source
from ranked

{% if is_incremental() %}
  qualify row_number()
    over (partition by creator_stats_id order by updated_at desc nulls last) = 1
{% endif %}
