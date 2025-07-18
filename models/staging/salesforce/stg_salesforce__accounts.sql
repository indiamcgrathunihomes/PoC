with

    source as (select * from {{ source("salesforce", "account") }}),

    source_excluding_deleted_records as (select * from source where is_deleted = false),

    renamed as (
        select
            id,
            name,
            associated_city_c as associated_city,
            billing_postal_code,
            total_student_portfolio_c as total_student_portfolio,
            account_type_c as account_type,
            record_type_id,
            date_closed_c as date_closed,
            date_won_c as date_won
        from source_excluding_deleted_records
    )

select *
from renamed
