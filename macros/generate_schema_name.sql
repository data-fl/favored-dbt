{% macro generate_schema_name(custom_schema_name, node) -%}
  {# In dbt Cloud, target.name comes from the Environment's "Target name" #}
  {% if target.name == 'prod' %}
    {# In production: use folder schemas exactly (CORE/STAGING/SUMMARY) #}
    {{ (custom_schema_name or target.schema) | upper }}
  {% else %}
    {# In Studio/dev: keep everything in the Environment's base schema, unchanged #}
    {{ target.schema }}
  {% endif %}
{%- endmacro %}
