{{ config(materialized='view', tags=['staging','tiktok']) }}

select
  lower(nullif(trim(handle_name), ''))::varchar              as handle_name,
  tiktok_id::varchar                                          as tiktok_id,
  lower(nullif(trim(email_address), ''))::varchar            as email_address,
  acct_create::date                                          as acct_create,
  nullif(trim(sec_id), '')::varchar                          as sec_id,
  handle_modify::date                                        as handle_modify,
  is_tts::boolean                                            as is_tts,
  nullif(trim(language), '')::varchar                        as language,
  is_commerce::boolean                                       as is_commerce,
  nullif(trim(category), '')::varchar                        as category,
  fan_cnt::number                                            as fan_cnt,
  follow_cnt::number                                         as follow_cnt,
  like_cnt::number                                           as like_cnt,
  vid_cnt::number                                            as vid_cnt,
  share_cnt::number                                          as share_cnt,
  friend_cnt::number                                         as friend_cnt,
  code::number                                               as code,
  create_date::timestamp_ntz                                          as create_date,
  last_update::timestamp_ntz                                          as last_update
from {{ source('RAW','HYDRATE') }}
