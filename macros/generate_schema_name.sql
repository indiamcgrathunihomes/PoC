{% macro generate_schema_name(custom_schema_name, node) -%}
  {# Read the DBT_ENV var you’ll define in each Cloud environment #}
  {% set env = env_var('DBT_ENV', default=target.schema) %}

  {# 1) If you’ve set +schema, decide based on env #}
  {% if custom_schema_name is not none %}
    
    {%- if env == 'prod' -%}
      {{ custom_schema_name }}
    {%- else -%}
      {{ env ~ '_' ~ custom_schema_name }}
    {%- endif %}

  {# 2) No +schema? Fall back to env (or target.schema) #}
  {% else %}
    {{ env }}
  {% endif %}
{%- endmacro %}