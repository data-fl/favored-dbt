{{ 
    config(
        materialized='table', 
        tags=['dim','tiktok']
    ) 
}}

select
  tiktok_id,
  handle_name as handle,
  handle_start as valid_from,
  handle_end   as valid_to, 
  is_current_owner
from {{ ref('stg_tiktok_id_handle_map_time') }}
