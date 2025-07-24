with
    accounts_created as (
        select
            acc.account_id,
            acc.name,
            acc.record_type_id,
            typ.name as record_type_name,
            acc.account_number_1,
            date(acc.created_date) as source_date,
            'account' as source_object,
            case
                when typ.name like '%Uni%' then 'unihomes'
                else 'split the bills'
            end as source_company,
            'created' as source_type
        from {{ ref('stg_salesforce__accounts') }} as acc
        join 
            {{ ref('stg_salesforce__record_types') }} as typ 
            on acc.record_type_id = typ.record_type_id
        where
            1 = 1
            and acc.name not like '%DUPE%'
            and (
                    (
                        acc.record_type_id = '0121R000001M7k7QAC' -- Unihomes Online
                        or (
                            acc.record_type_id = '0121R000001YgmpQAC' -- Unihomes Order Form
                            and acc.parent_company = '0013600000IA8TZAA1' -- Unihomes Direct
                        )
                    )
            or (
                acc.record_type_id = '0121R000001M7dVQAS' -- Split The Bills
                and group_size > 0
                and source != 'Migration'
            )
            )
    ),
    accounts_completed as (
         select
            acc.account_id,
            acc.name,
            acc.record_type_id,
            typ.name as record_type_name,
            acc.account_number_1,
            date(acc.unified_active_date) as source_date,
            'account' as source_object,
            case
                when typ.name like '%Uni%' then 'unihomes'
                else 'split the bills'
            end as source_company,
            'completed' as source_type
        from {{ ref('stg_salesforce__accounts') }} as acc
        join 
            {{ ref('stg_salesforce__record_types') }} as typ 
            on acc.record_type_id = typ.record_type_id
        where
            1 = 1
            and acc.name not like '%DUPE%'
            and acc.unified_active_date IS NOT NULL
            and 
                ( 
                    acc.record_type_id = '0121R000001M7k7QAC' -- Unihomes Online
                    or
                        (
                            acc.record_type_id = '0121R000001M7dVQAS' -- Split The Bills
                            and acc.group_size > 0
                            and acc.source != 'Migration'

                        )
            )
    ),
    accounts_cancelled as (
         select
            acc.account_id,
            acc.name,
            acc.record_type_id,
            typ.name as record_type_name,
            acc.account_number_1,
            date(acc.cancellation_date) as source_date,
            'account' as source_object,
            case
                when typ.name like '%Uni%' then 'unihomes'
                else 'split the bills'
            end as source_company,
            'cancelled' as source_type
        from {{ ref('stg_salesforce__accounts') }} as acc
        join 
            {{ ref('stg_salesforce__record_types') }} as typ 
            on acc.record_type_id = typ.record_type_id
        where
            1 = 1
            and acc.name not like '%DUPE%'
            and acc.cancellation_date IS NOT NULL
            and acc.record_type_id in (
                '0121R000001M7k7QAC', -- Unihomes Online
                '0121R000001YgmpQAC', -- Unihomes Order Form
                '0121R000001M7dVQAS' -- Split The Bills
            )
            and acc.status in (
                'Active',
                'Cancelled',
                'Closed'
            )
    ),
    leads_created as (
         select
            lea.lead_id,
            lea.name,
            lea.record_type_id,
            typ.name as record_type_name,
            null as account_number_1,
            date(lea.created_date) as source_date,
            'lead' as source_object,
            case
                when typ.name like '%Uni%' then 'unihomes'
                else 'split the bills'
            end as source_company,
            'created' as source_type
        from {{ ref('stg_salesforce__leads') }} as lea
        join 
            {{ ref('stg_salesforce__record_types') }} typ 
            on lea.record_type_id = typ.record_type_id
        where
            1 = 1
            and lea.record_type_id in (
                '0121R000001M7kCQAS', -- Unihomes Online
                '0121R000001M7k2QAC' -- Split The Bills
            )
            and lea.status not in (
                'Duplicate',
                'Incorrect Details'
            )
    ),
    combined_data_table as (
        select *
        from accounts_created
        union
        select *
        from accounts_completed
        union
        select *
        from accounts_cancelled
        union
        select *
        from leads_created
    )
select *
from combined_data_table