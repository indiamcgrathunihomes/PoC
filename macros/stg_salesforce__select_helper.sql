{% macro select_fields_structured(source_name, table_name) %}
  {#
    This macro pulls fields from a raw Salesforce object and:
    - Removes suffixes (__c, __cs, _c)
    - Converts to snake_case
    - Casts DATE and TIMESTAMP types appropriately
    - Deduplicates aliases safely
    - Reorders fields by DBT convention:
      Identifiers → Foreign Keys → Descriptive → Dates → Measures → Others
    - Renames the `id` field to `{table_name}_id`
  #}

  {% set columns = adapter.get_columns_in_relation(source(source_name, table_name)) %}
  {% set known_prefixes = ['sf_', 'custom_', 'salesforce_'] %}
  {% set cleaned_names = [] %}

  {# === Step 1: Clean and snake_case column names === #}
  {% for col in columns %}
    {% set raw_name = col.name %}
    {% set lower_name = raw_name.lower() %}

    {# Strip trailing suffixes: __cs, __c, _c #}
    {% if lower_name.endswith('__cs') %}
      {% set lower_name = lower_name[:-4] %}
    {% elif lower_name.endswith('__c') %}
      {% set lower_name = lower_name[:-3] %}
    {% elif lower_name.endswith('_c') %}
      {% set lower_name = lower_name[:-2] %}
    {% endif %}

    {# Strip prefixes #}
    {% for prefix in known_prefixes %}
      {% if lower_name.startswith(prefix) %}
        {% set lower_name = lower_name | replace(prefix, '') %}
      {% endif %}
    {% endfor %}

    {# Convert to snake_case (safe approximation) #}
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

  {# === Step 2: Deduplicate aliases if needed === #}
  {% set name_counts = {} %}
  {% for name in cleaned_names %}
    {% set count = name_counts.get(name, 0) %}
    {% do name_counts.update({name: count + 1}) %}
  {% endfor %}

  {% set running_counts = {} %}
  {% set seen_aliases = [] %}
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

    {# Make alias unique if needed #}
    {% if name_counts[clean_name] > 1 %}
      {% set count = running_counts.get(clean_name, 0) %}
      {% set alias = clean_name ~ '_' ~ count %}
      {% do running_counts.update({clean_name: count + 1}) %}
    {% endif %}

    {# Rename 'id' to {table_name}_id #}
    {% if alias == 'id' %}
      {% set alias = table_name ~ '_id' %}
    {% endif %}

    {% do seen_aliases.append(alias) %}
    {% set data_type = col.data_type.upper() %}

    {# Cast DATE and TIMESTAMP types #}
    {% if data_type == 'DATE' %}
      {% set line = 'cast(' ~ adapter.quote(raw_name) ~ ' as date) as ' ~ alias %}
    {% elif data_type in ['TIMESTAMP_NTZ', 'TIMESTAMP_TZ', 'TIMESTAMP_LTZ', 'DATETIME'] %}
      {% set line = 'cast(' ~ adapter.quote(raw_name) ~ ' as timestamp) as ' ~ alias %}
    {% else %}
      {% set line = adapter.quote(raw_name) ~ ' as ' ~ alias %}
    {% endif %}

    {# Categorize by DBT best-practice order #}
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

  {{ return((
      identifiers +
      foreign_keys +
      descriptive +
      dates +
      measures +
      others
    ) | join(',\n    ')) }}
{% endmacro %}
