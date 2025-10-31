{{ 
    config(
            materialized='view',
            tags=['staging','tiktok']
        ) 
}}

select
  tiktok_id::varchar                                          as tiktok_id,
  lower(nullif(trim(handle_name), ''))::varchar              as handle_name,
  nullif(trim(display_name), '')::varchar                    as display_name,
  lower(nullif(trim(email_address), ''))::varchar            as email_address,
  creator_ec_access_level::number                            as creator_ec_access_level,
  followers::number                                          as followers,
  nullif(trim(ec_level), '')::varchar                        as ec_level,
  pqp::float                                                 as pqp,
  nullif(trim(mcn_name), '')::varchar                        as mcn_name,
  top_product_1_id::number                                   as top_product_1_id,
  nullif(trim(top_product_1_name), '')::varchar              as top_product_1_name,
  top_product_2_id::number                                   as top_product_2_id,
  nullif(trim(top_product_2_name), '')::varchar              as top_product_2_name,
  top_product_3_id::number                                   as top_product_3_id,
  nullif(trim(top_product_3_name), '')::varchar              as top_product_3_name,
  top_category_1_id::number                                  as top_category_1_id,
  nullif(trim(top_category_1_name), '')::varchar             as top_category_1_name,
  top_category_1_revenue_percentage::float                   as top_category_1_revenue_percentage,
  top_category_2_id::number                                  as top_category_2_id,
  nullif(trim(top_category_2_name), '')::varchar             as top_category_2_name,
  top_category_2_revenue_percentage::float                   as top_category_2_revenue_percentage,
  top_category_3_id::number                                  as top_category_3_id,
  nullif(trim(top_category_3_name), '')::varchar             as top_category_3_name,
  top_category_3_revenue_percentage::float                   as top_category_3_revenue_percentage,
  code::number                                               as code,
  create_date::timestamp_ntz                                          as create_date,
  last_update::timestamp_ntz                                          as last_update
from {{ source('RAW','CREATORINFO') }}
