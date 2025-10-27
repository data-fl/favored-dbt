{{ config(
    materialized='view',
    tags=['dim','tiktok']
) }}

with ap as (
    select
        lower(nullif(trim(a.handle_name), ''))                    as handle_name,
        a.data_id,
        a.phone_number,
        nullif(trim(a.country), '')                               as country,
        a.state, a.city, a.zipcode, a.address_1, a.address_2
    from {{ ref('stg_address_phone') }} a
),
echo as (
    select lower(handle_name) as handle_name, tiktok_id
    from {{ ref('stg_echo') }}
    where tiktok_id is not null
),
joined as (
    select
        e.tiktok_id::varchar                                      as tiktok_id,
        ap.data_id,
        ap.phone_number,
        'address_phone'                                           as source,
        /* No timestamp in source; record when we materialize */
        current_timestamp()                                       as ingested_at
    from ap
    join echo e using (handle_name)
    where ap.phone_number is not null and trim(ap.phone_number) <> ''
),
normalized as (
    /* Minimal E.164 normalization (US/CA fallback only). See notes. */
    select
        tiktok_id,
        source,
        ingested_at,
        phone_number                                              as raw_phone,
        case
            when regexp_like(phone_number, '^\\s*\\+\\d{10,15}\\s*$') then
                regexp_replace(phone_number, '\\s+', '')
            else
                case
                    when regexp_replace(phone_number, '[^0-9]', '') rlike '^1?\\d{10}$' then
                        '+1' || right(regexp_replace(phone_number, '[^0-9]', ''), 10)
                    else null
                end
        end                                                        as phone_e164,
        data_id
    from joined
),
with_ids as (
    select
        /* phone_id = hash(phone_e164) */
        hash(phone_e164)                                          as phone_id,
        tiktok_id,
        phone_e164,
        raw_phone,
        source,
        ingested_at,
        data_id
    from normalized
    where phone_e164 is not null
),
ranked as (
    /* primary = newest; without a source timestamp we use ingested_at, then data_id desc */
    select
        w.*,
        row_number() over (
            partition by tiktok_id
            order by ingested_at desc nulls last, data_id desc, phone_id asc
        ) as rn
    from with_ids w
)

select
    tiktok_id,
    phone_id,
    phone_e164,
    raw_phone,
    (rn = 1) as is_primary,
    source,
    ingested_at
from ranked
