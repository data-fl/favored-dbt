{{ 
    config(
        materialized='view', 
        tags=['staging','tiktok']
    ) 
}}

select
  lower(nullif(trim(handle_name), ''))::varchar              as handle_name,
  nullif(trim(bio), '')::varchar                             as bio,
  nullif(trim(bio_url), '')::varchar                         as bio_url,
  creator_id::varchar                                         as creator_id,
  follower_count::number                                     as follower_count,
  lower(nullif(trim(email), ''))::varchar                    as email,
  nullif(trim(follower_ages), '')::varchar                   as follower_ages,
  nullif(trim(follower_genders), '')::varchar                as follower_genders,
  nullif(trim(follower_locations), '')::varchar              as follower_locations,
  nullif(trim(follower_trend), '')::varchar                  as follower_trend,
  nullif(trim(gmv_trend), '')::varchar                       as gmv_trend,
  nullif(trim(units_sold_trend), '')::varchar                as units_sold_trend,
  nullif(trim(video_views_trend), '')::varchar               as video_views_trend,
  nullif(trim(video_engagement_trend), '')::varchar          as video_engagement_trend,
  nullif(trim(shoppable_video_views_trend), '')::varchar     as shoppable_video_views_trend,
  nullif(trim(shoppable_video_engagement_trend), '')::varchar as shoppable_video_engagement_trend,
  nullif(trim(hashtags), '')::varchar                        as hashtags,
  nullif(trim(videos), '')::varchar                          as videos,
  nullif(trim(similar_creators_ids), '')::varchar            as similar_creators_ids,
  created_time::timestamp_ntz                                as created_time,
  last_updated_time::timestamp_ntz                           as last_updated_time,
  nullif(trim(gender), '')::varchar                          as gender,
  nullif(trim(language), '')::varchar                        as language,
  tiktok_id::varchar                                          as tiktok_id,
  ig_follower_count::number                                  as ig_follower_count,
  nullif(trim(creator_summary_texts), '')::varchar           as creator_summary_texts,
  nullif(trim(gmv_30d_num_history), '')::varchar             as gmv_30d_num_history,
  nullif(trim(is_discord_creator), '')::varchar              as is_discord_creator,
  nullif(trim(fts), '')::varchar                             as fts,
  nullif(trim(ethnicity), '')::varchar                       as ethnicity
from {{ source('RAW','ECHO') }}
