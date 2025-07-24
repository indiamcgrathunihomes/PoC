{% set prod_relation = api.Relation.create(
    database = "DATA_AND_ANALYTICS_PROD",
    schema = "BASE",
    identifier = "BASE_STUDENT_MARKETSHARE_AGENT_LIST",
    type = "table"
) %}

{% set dev_relation = ref('base_student_marketshare_agent_list') %}

{% set diff_sql %}
    {{ audit_helper.compare_all_columns(
        a_relation = prod_relation,
        b_relation = dev_relation,
        primary_key = "record_id"
    ) }}
{% endset %}

{% if execute %}
  {% set results = run_query(diff_sql) %}
  {% if results %}
    {% for row in results.rows %}
      {% set col = row[0] %}
      {% set match = row[1] %}
      {% set null_a = row[2] %}
      {% set null_b = row[3] %}
      {% set miss_a = row[4] %}
      {% set miss_b = row[5] %}
      {% set conflict = row[6] %}
      {% do log("📊 " ~ col ~
        " | Matches: " ~ match ~
        " | Nulls in A: " ~ null_a ~
        " | Nulls in B: " ~ null_b ~
        " | Missing A: " ~ miss_a ~
        " | Missing B: " ~ miss_b ~
        " | Conflicts: " ~ conflict, info=True) %}
    {% endfor %}
  {% else %}
    {% do log("✅ No differences found in compare_all_columns.", info=True) %}
  {% endif %}
{% endif %}

-- Also return results as a queryable table if needed
{% set rendered_query %}
  {{ diff_sql }}
{% endset %}

-- You can even copy-paste this to log it or use it in another query
{{ log(rendered_query, info=True) }}

{{ rendered_query }}