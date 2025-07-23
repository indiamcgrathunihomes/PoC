{% macro select_fields_structured(model_or_source, table_name=None) %}
  {% if table_name is none %}
    {# Called with ref() #}
    {% set relation = model_or_source %}
    {% set table_name = relation.identifier %}
  {% else %}
    {# Called with source() #}
    {% set relation = source(model_or_source, table_name) %}
  {% endif %}

  {% set columns = adapter.get_columns_in_relation(relation) %}
  {% set known_prefixes = ['sf_', 'custom_', 'salesforce_'] %}
  {% set cleaned_names = [] %}

  {# === Step 1: Clean and snake_case column names === #}
  {% for col in columns %}
    {% set raw_name = col.name %}
    {% set lower_name = raw_name.lower() %}

    {% if lower_name.endswith('__cs') %}
      {% set lower_name = lower_name[:-4] %}
    {% elif lower_name.endswith('__c') %}
      {% set lower_name = lower_name[:-3] %}
    {% elif lower_name.endswith('_c') %}
      {% set lower_name = lower_name[:-2] %}
    {% endif %}

    {% for prefix in known_prefixes %}
      {% if lower_name.startswith(prefix) %}
        {% set lower_name = lower_name | replace(prefix, '') %}
      {% endif %}
    {% endfor %}

    {% set lower_name = lower_name
      | replace(' ', '_') | replace('-', '_') | replace('.', '_') | replace('/', '_') | replace('\\', '_')
      | replace('(', '') | replace(')', '') | replace('[', '') | replace(']', '')
      | replace('{', '') | replace('}', '') | replace('@', '') | replace('#', '') | replace('!', '')
      | replace('$', '') | replace('%', '') | replace('^', '') | replace('&', '') | replace('*', '')
      | replace('+', '') | replace('=', '') | replace(':', '') | replace(';', '') | replace(',', '')
      | replace('?', '') | replace('\'', '') | replace('"', '') | replace('<', '') | replace('>', '') %}
    {% set lower_name = lower_name | replace('__', '_') %}
    {% set lower_name = lower_name.strip('_') %}

    {% do cleaned_names.append(lower_name) %}
  {% endfor %}

  {# === Step 2: Deduplicate aliases === #}
  {% set name_counts = {} %}
  {% for name in cleaned_names %}
    {% set count = name_counts.get(name, 0) %}
    {% do name_counts.update({name: count + 1}) %}
  {% endfor %}

  {% set running_counts = {} %}
  {% set identifiers = [] %}
  {% set foreign_keys = [] %}
  {% set descriptive = [] %}
  {% set dates = [] %}
  {% set measures = [] %}
  {% set others = [] %}

  {# === Step 3: Categorize and cast fields === #}
  {% for i in range(columns | length) %}
    {% set col = columns[i] %}
    {% set raw_name = col.name %}
    {% set clean_name = cleaned_names[i] %}
    {% set alias = clean_name %}

    {% if name_counts[clean_name] > 1 %}
      {% set count = running_counts.get(clean_name, 0) %}
      {% set alias = clean_name ~ '_' ~ count %}
      {% do running_counts.update({clean_name: count + 1}) %}
    {% endif %}

    {% if alias == 'id' %}
      {% set alias = table_name ~ '_id' %}
    {% endif %}

    {% set data_type = col.data_type.upper() %}
    {% if data_type == 'DATE' %}
      {% set line = 'cast(' ~ adapter.quote(raw_name) ~ ' as date) as ' ~ alias %}
    {% elif data_type in ['TIMESTAMP_NTZ', 'TIMESTAMP_TZ', 'TIMESTAMP_LTZ', 'DATETIME'] %}
      {% set line = 'cast(' ~ adapter.quote(raw_name) ~ ' as timestamp) as ' ~ alias %}
    {% else %}
      {% set line = adapter.quote(raw_name) ~ ' as ' ~ alias %}
    {% endif %}

    {% if alias == table_name ~ '_id' %}
      {% do identifiers.append(line) %}
    {% elif alias.endswith('_id') %}
      {% do foreign_keys.append(line) %}
    {% elif alias in ['name', 'description', 'type', 'industry'] %}
      {% do descriptive.append(line) %}
    {% elif data_type == 'DATE' or data_type in ['TIMESTAMP_NTZ', 'TIMESTAMP_TZ', 'TIMESTAMP_LTZ', 'DATETIME'] %}
      {% do dates.append(line) %}
    {% elif alias in ['amount', 'revenue', 'score', 'value'] or alias.endswith('_amount') or alias.endswith('_value') %}
      {% do measures.append(line) %}
    {% else %}
      {% do others.append(line) %}
    {% endif %}
  {% endfor %}

  {# === Step 4: Assemble with clear line breaks and comments === #}
  {% set parts = [] %}

  {% if identifiers %}
    {% do parts.append('-- Identifiers') %}
    {% do parts.append(identifiers | join(',\n    ')) %}
  {% endif %}

  {% if foreign_keys %}
    {% do parts.append('\n-- Foreign Keys') %}
    {% do parts.append(foreign_keys | join(',\n    ')) %}
  {% endif %}

  {% if descriptive %}
    {% do parts.append('\n-- Descriptive') %}
    {% do parts.append(descriptive | join(',\n    ')) %}
  {% endif %}

  {% if dates %}
    {% do parts.append('\n-- Dates') %}
    {% do parts.append(dates | join(',\n    ')) %}
  {% endif %}

  {% if measures %}
    {% do parts.append('\n-- Measures') %}
    {% do parts.append(measures | join(',\n    ')) %}
  {% endif %}

  {% if others %}
    {% do parts.append('\n-- Other Fields') %}
    {% do parts.append(others | join(',\n    ')) %}
  {% endif %}

  {{ return(parts | join(',\n\n    ')) }}
{% endmacro %}
