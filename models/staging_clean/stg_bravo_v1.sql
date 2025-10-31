{{ 
    config(
        materialized='view', 
        tags=['staging','tiktok']
    ) 
}}

select
  lower(nullif(trim(handle_name), ''))::varchar              as handle_name,
  tiktok_id::varchar                                          as tiktok_id,
  created_at::timestamp_ntz                                  as created_at,
  updated_at::timestamp_ntz                                  as updated_at,
  lower(nullif(trim(email), ''))::varchar                    as email,
  nullif(trim(other_fixed_traits), '')::varchar              as other_fixed_traits,
  nullif(trim(primary_content_categories), '')::varchar      as primary_content_categories,
  nullif(trim(age_presentation), '')::varchar                as age_presentation,
  nullif(trim(script_creativity), '')::varchar               as script_creativity,
  nullif(trim(branding_integration), '')::varchar            as branding_integration,
  nullif(trim(call_to_actions), '')::varchar                 as call_to_actions,
  nullif(trim(pacing), '')::varchar                          as pacing,
  nullif(trim(storytelling_structure), '')::varchar          as storytelling_structure,
  nullif(trim(charisma_authenticity), '')::varchar           as charisma_authenticity,
  nullif(trim(relatability), '')::varchar                    as relatability,
  nullif(trim(luxury_presentation), '')::varchar             as luxury_presentation,
  nullif(trim(real_life_contexts), '')::varchar              as real_life_contexts,
  nullif(trim(tiktok_native_effects), '')::varchar           as tiktok_native_effects,
  nullif(trim(scarcity_urgency), '')::varchar                as scarcity_urgency,
  nullif(trim(authority_trust_building), '')::varchar        as authority_trust_building,
  nullif(trim(adoption_of_trends), '')::varchar              as adoption_of_trends,
  nullif(trim(seasonal_cultural_relevance), '')::varchar     as seasonal_cultural_relevance
from {{ source('RAW','BRAVO_V1') }}
