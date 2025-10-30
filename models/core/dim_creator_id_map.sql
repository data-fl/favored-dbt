{{ 
    config(
        materialized='table', 
        tags=['core','id_mapping']
    ) 
}}

with src as (

    -- Echo creator_id → mapped to TikTok when available in same row
    select
        t.tiktok_id::varchar                    as tiktok_id,
        'tiktok_creator_id'                       as id_type,
        regexp_replace(
            regexp_replace(t.creator_id::varchar, '^(temp_temp_|tiktok_)', ''),  -- remove prefixes
            '_.*$',                                                              -- remove suffixes
            ''
        )                                       as id,
        'echo'                                  as id_source
    from {{ ref('stg_echo') }} as t
    where t.creator_id is not null

),

final as (

    select
        -- deterministic surrogate key
        md5(coalesce(id_type, '') || '|' || coalesce(id, '') || '|' || coalesce(id_source, '')) as map_id,
        tiktok_id,
        id_type,
        id,
        id_source
    from  src

)

select
    map_id,
    tiktok_id,
    id_type,
    id,
    id_source
from final
