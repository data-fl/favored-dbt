{{ config(
    materialized='incremental',
    unique_key=['tiktok_id','stat_name','updated_at'],
    tags=['dim','tiktok']
) }}

with creatorinfo as (
    select
        tiktok_id::varchar as tiktok_id,
        coalesce(last_update, create_date)::timestamp_ntz as updated_at,
        /* CAST ALL STATS TO VARCHAR */
        to_varchar(creator_ec_access_level) as creator_ec_access_level,
        to_varchar(followers)               as followers,
        nullif(trim(ec_level), '')          as ec_level,
        to_varchar(pqp)                     as pqp
    from {{ source('RAW','CREATORINFO') }}
),

echo as (
    select
        tiktok_id::varchar as tiktok_id,
        coalesce(last_updated_time, created_time)::timestamp_ntz as updated_at,
        to_varchar(follower_count)   as follower_count,
        to_varchar(ig_follower_count) as ig_follower_count
    from {{ source('RAW','ECHO') }}
),

hydrate as (
    select
        tiktok_id::varchar as tiktok_id,
        coalesce(last_update, create_date)::timestamp_ntz as updated_at,
        to_varchar(fan_cnt)    as fan_cnt,
        to_varchar(follow_cnt) as follow_cnt,
        to_varchar(like_cnt)   as like_cnt,
        to_varchar(vid_cnt)    as vid_cnt,
        to_varchar(share_cnt)  as share_cnt,
        to_varchar(friend_cnt) as friend_cnt
    from {{ source('RAW','HYDRATE') }}
),

market as (
    select
        tiktok_id::varchar as tiktok_id,
        coalesce(last_update, create_date)::timestamp_ntz as updated_at,
        to_varchar(followerCount)             as followerCount,
        to_varchar(promotedProductNum)        as promotedProductNum,
        to_varchar(brandCollaborationCount)   as brandCollaborationCount,
        to_varchar(avgCommissionRate)         as avgCommissionRate,
        to_varchar(avgEcLiveCommentCount)     as avgEcLiveCommentCount,
        to_varchar(avgEcLiveLikeCount)        as avgEcLiveLikeCount,
        to_varchar(avgEcLiveShareCount)       as avgEcLiveShareCount,
        to_varchar(avgEcLiveViewCount)        as avgEcLiveViewCount,
        to_varchar(avgEcVideoCommentCount)    as avgEcVideoCommentCount,
        to_varchar(avgEcVideoLikeCount)       as avgEcVideoLikeCount,
        to_varchar(avgEcVideoPlayCount)       as avgEcVideoPlayCount,
        to_varchar(avgEcVideoShareCount)      as avgEcVideoShareCount,
        to_varchar(gmv_amount)                as gmv_amount,
        to_varchar(gpm_amount)                as gpm_amount,
        to_varchar(liveGmv_amount)            as liveGmv_amount,
        to_varchar(liveGpm_amount)            as liveGpm_amount,
        to_varchar(videoGmv_amount)           as videoGmv_amount,
        to_varchar(videoGpm_amount)           as videoGpm_amount,
        to_varchar(unitsSold)                 as unitsSold,
        to_varchar(ecLiveCount)               as ecLiveCount,
        to_varchar(ecVideoCount)              as ecVideoCount,
        to_varchar(ecLiveEngagementRate)      as ecLiveEngagementRate
    from {{ source('RAW','MARKET') }}
),

u_creatorinfo as (
    select tiktok_id, updated_at, stat_name, stat_value
    from creatorinfo
    unpivot (
        stat_value for stat_name in (
            creator_ec_access_level,
            followers,
            ec_level,
            pqp
        )
    )
),
u_echo as (
    select tiktok_id, updated_at, stat_name, stat_value
    from echo
    unpivot (
        stat_value for stat_name in (
            follower_count,
            ig_follower_count
        )
    )
),
u_hydrate as (
    select tiktok_id, updated_at, stat_name, stat_value
    from hydrate
    unpivot (
        stat_value for stat_name in (
            fan_cnt,
            follow_cnt,
            like_cnt,
            vid_cnt,
            share_cnt,
            friend_cnt
        )
    )
),
u_market as (
    select tiktok_id, updated_at, stat_name, stat_value
    from market
    unpivot (
        stat_value for stat_name in (
            followerCount,
            promotedProductNum,
            brandCollaborationCount,
            avgCommissionRate,
            avgEcLiveCommentCount,
            avgEcLiveLikeCount,
            avgEcLiveShareCount,
            avgEcLiveViewCount,
            avgEcVideoCommentCount,
            avgEcVideoLikeCount,
            avgEcVideoPlayCount,
            avgEcVideoShareCount,
            gmv_amount,
            gpm_amount,
            liveGmv_amount,
            liveGpm_amount,
            videoGmv_amount,
            videoGpm_amount,
            unitsSold,
            ecLiveCount,
            ecVideoCount,
            ecLiveEngagementRate
        )
    )
),

unioned as (
    select * from u_creatorinfo
    union all
    select * from u_echo
    union all
    select * from u_hydrate
    union all
    select * from u_market
),

clean as (
    select
        tiktok_id,
        lower(stat_name) as stat_name,
        nullif(trim(stat_value), '') as stat_value,
        updated_at
    from unioned
    where stat_value is not null
),

final as (
    select
        tiktok_id,
        stat_name,
        stat_value,         -- stored as VARCHAR; parse downstream if needed
        updated_at,
        case when updated_at = max(updated_at) over (partition by tiktok_id, stat_name)
             then true else false end as is_current
    from clean
)

select * from final