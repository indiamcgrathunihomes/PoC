{% macro audit_model_vs_prod(
    model_name,
    prod_db,
    prod_schema,
    prod_table,
    primary_key
) %}
    {% set prod_relation = api.Relation.create(
        database = prod_db,
        schema = prod_schema,
        identifier = prod_table,
        type = "table"
    ) %}

    {% set dev_relation = ref(model_name) %}

    {% do log("🔎 Comparing PROD: " ~ prod_relation ~ " vs DEV: " ~ dev_relation, info=True) %}

    select 'row_count_mismatch' as check_type, *
    from {{ audit_helper.compare_row_counts(
        a_relation = prod_relation,
        b_relation = dev_relation
    ) }}

    union all

    select 'column_name_mismatch' as check_type, *
    from (
        select column_name
        from {{ prod_relation.database }}.information_schema.columns
        where table_schema = '{{ prod_relation.schema }}'
          and table_name = '{{ prod_relation.identifier }}'
        except
        select column_name
        from {{ dev_relation.database }}.information_schema.columns
        where table_schema = '{{ dev_relation.schema }}'
          and table_name = '{{ dev_relation.identifier }}'
    )

    union all

    select 'value_mismatch' as check_type, *
    from {{ audit_helper.compare_all_columns(
        a_relation = prod_relation,
        b_relation = dev_relation,
        primary_key = primary_key
    ) }}

{% endmacro %}
