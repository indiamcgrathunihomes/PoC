{% macro log_audit_summary(prod_relation_str, dev_relation_ref, primary_key) %}

  {% set prod_relation = adapter.get_relation(
    database=prod_relation_str.split('.')[0],
    schema=prod_relation_str.split('.')[1],
    identifier=prod_relation_str.split('.')[2]
  ) %}

  {% set dev_relation = dev_relation_ref %}

  {% if prod_relation is none %}
    {% do log("❌ PROD relation not found: " ~ prod_relation_str, info=True) %}
    {% do return(None) %}
  {% endif %}

  {% set compare_result = audit_helper.compare_all_columns(
    a_relation = prod_relation,
    b_relation = dev_relation,
    primary_key = primary_key
  ) %}

  {% set results = run_query(compare_result).columns %}
  {% if execute %}
    {% do log("\n🔍 AUDIT SUMMARY for " ~ prod_relation_str ~ " vs " ~ dev_relation.database ~ "." ~ dev_relation.schema ~ "." ~ dev_relation.identifier, info=True) %}
    {% for row in load_result(compare_result)['data'] %}
      {% set column_name = row[0] %}
      {% set perfect_match = row[1] %}
      {% set null_in_a = row[2] %}
      {% set null_in_b = row[3] %}
      {% set missing_from_a = row[4] %}
      {% set missing_from_b = row[5] %}
      {% set conflicting_values = row[6] %}

      {% do log(
        "📊 " ~ column_name ~
        " | Matches: " ~ perfect_match ~
        " | Nulls in A: " ~ null_in_a ~
        " | Nulls in B: " ~ null_in_b ~
        " | Missing A: " ~ missing_from_a ~
        " | Missing B: " ~ missing_from_b ~
        " | Conflicts: " ~ conflicting_values, info=True) %}
    {% endfor %}
  {% endif %}

{% endmacro %}
