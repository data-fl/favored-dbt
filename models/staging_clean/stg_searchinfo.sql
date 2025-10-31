{{ 
    config(
        materialized='view', 
        tags=['staging','tiktok']
    ) 
}}

select
  lower(nullif(trim(handle_name), ''))::varchar              as handle_name,
  nullif(trim(display_name), '')::varchar                    as display_name,
  nullif(trim(selectionRegion), '')::varchar                 as selectionRegion,
  avgEcLiveUv::number                                        as avgEcLiveUv,
  avgEcVideoViewCount::number                                as avgEcVideoViewCount,
  followerCount::number                                      as followerCount,
  gmv_amount::float                                          as gmv_amount,
  gmvRange_minimumAmount::float                              as gmvRange_minimumAmount,
  gmvRange_maximumAmount::float                              as gmvRange_maximumAmount,
  liveGmv_amount::float                                      as liveGmv_amount,
  videoGmv_amount::float                                     as videoGmv_amount,
  unitsSoldRange_minimumAmount::number                       as unitsSoldRange_minimumAmount,
  unitsSoldRange_maximumAmount::number                       as unitsSoldRange_maximumAmount,
  categoryIds_1::number                                      as categoryIds_1,
  categoryIds_2::number                                      as categoryIds_2,
  categoryIds_3::number                                      as categoryIds_3,
  nullif(trim(topFollowerDemographics_majorGender_gender), '')::varchar as topFollowerDemographics_majorGender_gender,
  topFollowerDemographics_majorGender_percentage::number     as topFollowerDemographics_majorGender_percentage,
  create_date::timestamp_ntz                                          as create_date,
  last_update::timestamp_ntz                                          as last_update
from {{ source('RAW','SEARCHINFO') }}
