{{ config(
    materialized='view',
    tags=['dim','tiktok']
) }}

with echo_emails as (
    select
        e.tiktok_id::varchar                                      as tiktok_id,
        lower(nullif(trim(e.email), ''))                          as email_normalized,
        nullif(trim(e.email), '')                                 as email_raw,
        coalesce(e.last_updated_time, e.created_time)             as ingested_at,
        'echo'                                                    as source
    from {{ ref('stg_echo') }} e
    where e.email is not null and trim(e.email) <> ''
),

with_ids as (
    select
        /* creator_email_id = hash(email_normalized, purpose, tiktok_id) */
        hash(email_normalized, tiktok_id)                as creator_email_id,
        tiktok_id,
        email_normalized,
        email_raw,
        source,
        ingested_at
    from echo_emails
),

ranked as (
    /* primary = latest by tiktok_id using real timestamps from echo */
    select
        w.*,
        row_number() over (
            partition by tiktok_id
            order by ingested_at desc nulls last, creator_email_id asc
        ) as rn
    from with_ids w
)

select
    creator_email_id,
    tiktok_id,
    email_normalized,
    email_raw,
    source,
    ingested_at,
    (rn = 1) as is_primary
from ranked
