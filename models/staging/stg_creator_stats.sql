{{ 
    config(
        materialized='view', 
        tags=['staging','tiktok','stats']
    ) 
}}

-- Priority to break ties when updated_at is equal
with source_priority as (

  select * from values
    ('market', 1),
    ('searchinfo', 2),
    ('echo', 3),
    ('hydrate', 4),
    ('creatorinfo', 5),
    ('crmcreator', 6)
  as t(source, priority)

),

-- Temporal handle → tiktok_id map (staging, not dim)
handle_map as (

  select
    lower(handle_name) as handle_name,
    tiktok_id,
    handle_start,
    handle_end,
    is_current_owner
  from {{ ref('stg_tiktok_id_handle_map_time') }}

),

/* =========================
   1) Source CTEs (cast unpivoted stats to FLOAT)
   ========================= */

-- stg_creatorinfo (has tiktok_id)
src_creatorinfo as (

  select
    'creatorinfo' as source,
    t.tiktok_id,
    t.create_date as created_at,
    t.last_update as updated_at,
    /* force types for UNPIVOT */
    t.creator_ec_access_level::float as creator_ec_access_level,
    t.followers::float               as followers,
    t.pqp::float                     as pqp
  from {{ ref('stg_creatorinfo') }} t

),

-- stg_echo (has tiktok_id)
src_echo as (

  select
    'echo' as source,
    e.tiktok_id,
    e.created_time      as created_at,
    e.last_updated_time as updated_at,
    e.follower_count::float    as follower_count,
    e.ig_follower_count::float as ig_follower_count
  from {{ ref('stg_echo') }} e
  where e.tiktok_id is not null

),

-- stg_hydrate (has tiktok_id and *_cnt) → alias *_cnt -> *_count, cast to float
src_hydrate as (

  select
    'hydrate' as source,
    h.tiktok_id,
    h.create_date as created_at,
    h.last_update as updated_at,
    h.fan_cnt::float    as fan_count,
    h.follow_cnt::float as follow_count,
    h.like_cnt::float   as like_count,
    h.vid_cnt::float    as vid_count,
    h.share_cnt::float  as share_count,
    h.friend_cnt::float as friend_count
  from {{ ref('stg_hydrate') }} h
  where h.tiktok_id is not null

),

-- stg_market (has tiktok_id) → cast numbers to float
src_market as (

  select
    'market' as source,
    m.tiktok_id,
    m.create_date as created_at,
    m.last_update as updated_at,
    m.followerCount::float           as follower_count,
    m.promotedProductNum::float      as promoted_product_num,
    m.brandCollaborationCount::float as brand_collaboration_count,
    m.avgCommissionRate::float       as avg_commission_rate,
    m.avgEcLiveCommentCount::float   as avg_ec_live_comment_count,
    m.avgEcLiveLikeCount::float      as avg_ec_live_like_count,
    m.avgEcLiveShareCount::float     as avg_ec_live_share_count,
    m.avgEcLiveViewCount::float      as avg_ec_live_view_count,
    m.avgEcVideoCommentCount::float  as avg_ec_video_comment_count,
    m.avgEcVideoLikeCount::float     as avg_ec_video_like_count,
    m.avgEcVideoPlayCount::float     as avg_ec_video_play_count,
    m.avgEcVideoShareCount::float    as avg_ec_video_share_count,
    m.gmv_amount::float              as gmv_amount,
    m.gpm_amount::float              as gpm_amount,
    m.liveGmv_amount::float          as live_gmv_amount,
    m.liveGpm_amount::float          as live_gpm_amount,
    m.videoGmv_amount::float         as video_gmv_amount,
    m.videoGpm_amount::float         as video_gpm_amount,
    m.unitsSold::float               as units_sold,
    m.ecLiveCount::float             as ec_live_count,
    m.ecVideoCount::float            as ec_video_count,
    m.ecLiveEngagementRate::float    as ec_live_engagement_rate
  from {{ ref('stg_market') }} m

),

-- stg_crmcreator (NO tiktok_id) → resolve via temporal handle map; cast followers
src_crmcreator as (

  with base as (
    select
      'crmcreator' as source,
      lower(c.handle_name) as handle_name,
      c.create_date as created_at,
      c.last_update as updated_at,
      c.followers::float   as followers
    from {{ ref('stg_crmcreator') }} c
  ),
  resolved as (
    
    select
      hm.tiktok_id,
      b.source,
      b.created_at,
      b.updated_at,
      b.followers
    from base b
    left join handle_map hm
      on hm.handle_name = b.handle_name
     and (
          (hm.handle_end  is null and coalesce(b.updated_at, b.created_at) >= hm.handle_start)
          or
          (hm.handle_end is not null
           and coalesce(b.updated_at, b.created_at) >= hm.handle_start
           and coalesce(b.updated_at, b.created_at) <  hm.handle_end)
         )
  )
  select * from resolved

),

-- stg_searchinfo (NO tiktok_id) → resolve via temporal handle map; cast all stats to float
src_searchinfo as (
  
  with base as (
    select
      'searchinfo' as source,
      lower(s.handle_name)           as handle_name,
      s.create_date                  as created_at,
      s.last_update                  as updated_at,
      s.avgEcLiveUv::float           as avg_ec_live_uv,
      s.avgEcVideoViewCount::float   as avg_ec_video_view_count,
      s.followerCount::float         as follower_count,
      s.gmv_amount::float            as gmv_amount,
      s.gmvRange_minimumAmount::float as gmv_range_minimum_amount,
      s.gmvRange_maximumAmount::float as gmv_range_maximum_amount,
      s.liveGmv_amount::float        as live_gmv_amount,
      s.videoGmv_amount::float       as video_gmv_amount,
      s.unitsSoldRange_minimumAmount::float as units_sold_range_minimum_amount,
      s.unitsSoldRange_maximumAmount::float as units_sold_range_maximum_amount
    from {{ ref('stg_searchinfo') }} s
  
  ),
  resolved as (
    
    select
      hm.tiktok_id,
      b.source,
      b.created_at,
      b.updated_at,
      b.avg_ec_live_uv,
      b.avg_ec_video_view_count,
      b.follower_count,
      b.gmv_amount,
      b.gmv_range_minimum_amount,
      b.gmv_range_maximum_amount,
      b.live_gmv_amount,
      b.video_gmv_amount,
      b.units_sold_range_minimum_amount,
      b.units_sold_range_maximum_amount
    from base b
    left join handle_map hm
      on hm.handle_name = b.handle_name
     and (
          (hm.handle_end  is null and coalesce(b.updated_at, b.created_at) >= hm.handle_start)
          or
          (hm.handle_end is not null
           and coalesce(b.updated_at, b.created_at) >= hm.handle_start
           and coalesce(b.updated_at, b.created_at) <  hm.handle_end)
         )
  )
  select * from resolved

),

/* =========================
   2) UNPIVOT to tidy format
   ========================= */

u_creatorinfo as (
  
  select tiktok_id, created_at, updated_at, 'creatorinfo' as source, stat_name, stat_value
  from src_creatorinfo
  unpivot (stat_value for stat_name in (
    creator_ec_access_level,
    followers,
    pqp
  ))

),

u_echo as (

  select tiktok_id, created_at, updated_at, 'echo' as source, stat_name, stat_value
  from src_echo
  unpivot (stat_value for stat_name in (
    follower_count,
    ig_follower_count
  ))

),

u_hydrate as (
  
  select tiktok_id, created_at, updated_at, 'hydrate' as source, stat_name, stat_value
  from src_hydrate
  unpivot (stat_value for stat_name in (
    fan_count,
    follow_count,
    like_count,
    vid_count,
    share_count,
    friend_count
  ))

),

u_market as (
  
  select tiktok_id, created_at, updated_at, 'market' as source, stat_name, stat_value
  from src_market
  unpivot (stat_value for stat_name in (
    follower_count,
    promoted_product_num,
    brand_collaboration_count,
    avg_commission_rate,
    avg_ec_live_comment_count,
    avg_ec_live_like_count,
    avg_ec_live_share_count,
    avg_ec_live_view_count,
    avg_ec_video_comment_count,
    avg_ec_video_like_count,
    avg_ec_video_play_count,
    avg_ec_video_share_count,
    gmv_amount,
    gpm_amount,
    live_gmv_amount,
    live_gpm_amount,
    video_gmv_amount,
    video_gpm_amount,
    units_sold,
    ec_live_count,
    ec_video_count,
    ec_live_engagement_rate
  ))

),

u_crmcreator as (
  
  select tiktok_id, created_at, updated_at, 'crmcreator' as source, stat_name, stat_value
  from src_crmcreator
  unpivot (stat_value for stat_name in (
    followers
  ))

),

u_searchinfo as (
  
  select tiktok_id, created_at, updated_at, 'searchinfo' as source, stat_name, stat_value
  from src_searchinfo
  unpivot (stat_value for stat_name in (
    avg_ec_live_uv,
    avg_ec_video_view_count,
    follower_count,
    gmv_amount,
    gmv_range_minimum_amount,
    gmv_range_maximum_amount,
    live_gmv_amount,
    video_gmv_amount,
    units_sold_range_minimum_amount,
    units_sold_range_maximum_amount
  ))

),

unioned as (
  
  select * from u_creatorinfo
  union all select * from u_echo
  union all select * from u_hydrate
  union all select * from u_market
  union all select * from u_crmcreator
  union all select * from u_searchinfo

),

/* =========================
   3) Final tidy output
   ========================= */
final as (

  select
    tiktok_id,
    lower(stat_name) as stat_name,
    stat_value,     -- float
    created_at,
    updated_at,
    source
  from unioned
  where stat_value is not null

)

select
  f.*,
  sp.priority as source_priority
from final f
left join source_priority sp
  on sp.source = f.source
where tiktok_id is not null
