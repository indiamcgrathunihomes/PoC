

-- Rebuilding query for https://unihomes.lightning.force.com/lightning/r/Report/00OUc0000069OinMAE/view
WITH active_customers AS (
    SELECT
        acc.account_id AS salesforce_18_digit_id,
        acc.account_id AS record_id,
        acc.name AS company,
        acc.name AS name,
        acc.associated_city,
        acc.billing_postal_code AS postcode,
        acc.total_student_portfolio,
        acc.account_type,
        'Customer' AS category,
        'Account' AS sf_object,
        NULL AS phone,
        NULL as competitor,
        typ.name AS record_type,
        NULL AS email,
        NULL AS stage,
        DATE(date_won) AS source_date
    FROM
        {{ ref ('stg_salesforce__accounts')}} acc
        LEFT JOIN {{ ref('stg_salesforce__record_types') }} typ ON acc.record_type_id = typ.record_type_id
    WHERE
        1=1
        AND acc.record_type_id = '012360000009cYwAAI' -- Need to pull in Record Type object to do this dynamically. Filters for Landlord/Agent 
        AND acc.date_closed IS NULL
        AND acc.date_won IS NOT NULL
),

active_opps AS (
-- Rebuilding query for https://unihomes.lightning.force.com/lightning/r/Report/00OUc0000069QmDMAU/view
 SELECT
        acc.account_id AS salesforce_18_digit_id,
        opp.opportunity_id AS record_id,
        acc.name AS company,
        acc.name AS name,
        acc.associated_city,
        acc.billing_postal_code AS postcode,
        qty.total_student_portfolio,
        acc.account_type,
        'Prospect' AS category,
        'Opportunity' AS sf_object,
        NULL AS phone,
        opp.competitor_name AS competitor,
        typ.name AS record_type,
        NULL AS email,
        opp.stage_name AS stage,
        DATE(opp.created_date) AS source_date
    FROM
        {{ ref ('stg_salesforce__opportunities')}} opp
        LEFT JOIN {{ ref ('stg_salesforce__accounts')}} acc ON opp.account_id = acc.account_id
        LEFT JOIN
                    (
                        SELECT
                            opportunity_id,
                            SUM(quantity_0) AS total_student_portfolio
                        FROM
                            {{ ref ('stg_salesforce__opportunity_products')}}
                        WHERE
                            portfolio_type = 'Student'
                        GROUP BY
                            opportunity_id
                    )   qty ON qty.opportunity_id = opp.opportunity_id
        LEFT JOIN {{ ref('stg_salesforce__record_types') }} typ ON opp.record_type_id = typ.record_type_id
    WHERE
        1=1
        AND acc.record_type_id = '012360000009cYwAAI' -- Need to pull in Record Type object to do this dynamically. Filters for Landlord/Agent
        AND acc.date_closed IS NULL
        AND acc.date_won IS NULL
        AND opp.stage_name IN ('Renewal','Fact Find','Meeting Booked','Negotiation','Agreement Preparation','Agreement Sent','Trial')

),

valid_leads AS (
-- Rebuilding query for https://unihomes.lightning.force.com/lightning/r/Report/00OUc0000069OyvMAE/view
 SELECT
        lea.lead_id AS salesforce_18_digit_id,
        lea.lead_id AS record_id,
        lea.company,
        lea.name AS name,
        lea.associated_city,
        lea.postal_code AS postcode,
        lea.percentage_student AS total_student_portfolio,
        NULL AS account_type,
        'Prospect' AS category,
        'Lead' AS sf_object,
        lea.phone,
        lea.competitor_name AS competitor,
        typ.name AS record_type,
        lea.email,
        NULL AS stage,
        DATE(lea.created_date) AS source_date
    FROM
         {{ ref ('stg_salesforce__leads')}} lea
         LEFT JOIN {{ ref('stg_salesforce__record_types') }} typ ON lea.record_type_id = typ.record_type_id
    WHERE
        1=1
        AND lea.record_type_id = '012Uc000000Gue9IAC'  -- Need to pull in Record Type object to do this dynamically. Filters for Landlord/Agent
        AND lea.main_contact = TRUE
        AND lea.status <> 'Converted'
        AND lea.status IN ('Unqualified this Season','New','Working','Nurture')

),

combined_data_table AS (
SELECT * FROM active_customers
UNION
SELECT * FROM active_opps
UNION
SELECT * FROM valid_leads
)

SELECT * FROM combined_data_table