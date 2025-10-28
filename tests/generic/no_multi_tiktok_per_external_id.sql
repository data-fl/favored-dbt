{% test no_multi_tiktok_per_external_id(model) %}
    select
        id_type,
        id
    from {{ model }}
    where tiktok_id is not null
    group by 1,2
    having count(distinct tiktok_id) > 1
{% endtest %}
