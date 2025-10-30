{{ config(materialized='view', tags=['staging','tiktok']) }}

select
  lower(nullif(trim(handle_name), ''))::varchar              as handle_name,
  creator_id::varchar                                         as creator_id,
  nullif(trim(display_name), '')::varchar                    as display_name,
  followers::number                                          as followers,
  nullif(trim(category1_name), '')::varchar                  as category1_name,
  category1_id::number                                       as category1_id,
  nullif(trim(category2_name), '')::varchar                  as category2_name,
  category2_id::number                                       as category2_id,
  nullif(trim(category3_name), '')::varchar                  as category3_name,
  category3_id::number                                       as category3_id,
  code::number                                               as code,
  create_date::timestamp_ntz                                          as create_date,
  last_update::timestamp_ntz                                          as last_update
from {{ source('RAW','CRMCREATOR') }}

