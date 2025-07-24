--Uses audit_model_vs_prod_macro
--Prints detailed results to log
-- Change names accordingly for models

{{ audit_model_vs_prod(
    model_name = 'base_student_marketshare_agent_list', 
    prod_db = 'DATA_AND_ANALYTICS_PROD',
    prod_schema = 'BASE',
    prod_table = 'BASE_STUDENT_MARKETSHARE_AGENT_LIST',
    primary_key = 'record_id'
) }}