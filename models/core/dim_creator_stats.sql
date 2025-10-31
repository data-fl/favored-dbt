{{ 
    config(
        materialized='table',
        tags=['dim','tiktok','stats','current']
    ) 
}}

with latest as (

  select
    tiktok_id,
    stat_name,
    stat_value::float as stat_value,
    created_at,
    updated_at,
    source,
    -- pick the winner per (creator, stat)
    row_number() over (
      partition by tiktok_id, stat_name
      order by updated_at desc nulls last,
               case when source = 'market' then 1
                    when source = 'searchinfo' then 2
                    when source = 'echo' then 3
                    when source = 'hydrate' then 4
                    when source = 'creatorinfo' then 5
                    when source = 'crmcreator' then 6
                    else 99 end asc
    ) as rn
  from {{ ref('dim_creator_stats_log') }}

)

select
  -- stable key per (creator, stat)
  md5(coalesce(tiktok_id,'') || '|' || coalesce(stat_name,'')) as creator_stats_id,
  tiktok_id,
  stat_name,
  stat_value,
  created_at,
  updated_at,
  true as is_current,
  source
from latest
where rn = 1
