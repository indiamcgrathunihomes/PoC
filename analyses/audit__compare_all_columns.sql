{% set prod_relation = api.Relation.create(
    database = "DATA_AND_ANALYTICS_PROD",
    schema = "BASE",
    identifier = "BASE_STUDENT_MARKETSHARE_AGENT_LIST",
    type = "table"
) %}

{% set dev_relation = ref('base_student_marketshare_agent_list') %}

-- Log for verification
{% do log("prod_relation (manually built): " ~ prod_relation, info=True) %}
{% do log("dev_relation: " ~ dev_relation, info=True) %}

select *
from {{ audit_helper.compare_all_columns(
    a_relation = prod_relation,
    b_relation = dev_relation,
    primary_key = "record_id"
) }}
