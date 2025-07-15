{% macro generate_schema_name(custom_schema_name, node) -%}
  
  {# 1) If you’ve set +schema in your project or model, use exactly that #}
  {% if custom_schema_name is not none %}
    {{ custom_schema_name }}

  {# 2) Otherwise fall back to the default (target schema) #}
  {% else %}
    {{ target.schema }}
  {% endif %}

{%- endmacro %}