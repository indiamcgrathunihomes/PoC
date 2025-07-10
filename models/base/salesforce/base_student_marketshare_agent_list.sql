

-- Rebuilding query for https://unihomes.lightning.force.com/lightning/r/Report/00OUc0000069OinMAE/view
WITH active_customers AS (
    SELECT
        id AS salesforce_18_digit_id,
        name AS company,
        associated_city,
        billing_postal_code AS postcode,
        billing_latitude AS latitude,
        billing_longitude AS longitude,
        total_student_portfolio,
        account_type,
        'Customer' AS category,
        'Account' AS sf_object
    FROM
      {{ ref ('staging_salesforce_account')}}
    WHERE
      record_type_id = '012360000009cYwAAI' -- Need to pull in Record Type object to do this dynamically. Filters for Landlord/Agent 
      AND date_closed IS NULL
        AND date_won IS NOT NULL
),

active_opps AS (
-- Rebuilding query for https://unihomes.lightning.force.com/lightning/r/Report/00OUc0000069QmDMAU/view
 SELECT
        ac.id AS salesforce_18_digit_id,
        ac.name AS company,
        ac.associated_city_c AS associated_city,
        ac.billing_postal_code AS postcode,
        ac.billing_latitude AS latitude,
        ac.billing_longitude AS longitude,
        ac.total_student_portfolio_c AS total_student_portfolio,
        ac.account_type_c AS account_type,
        'Prospect' AS category,
        'Opportunity' AS sf_object
    FROM
        salesforce.opportunity op
    LEFT JOIN salesforce.account ac ON op.account_id = ac.id
    WHERE
        ac.record_type_id = '012360000009cYwAAI' -- Need to pull in Record Type object to do this dynamically. Filters for Landlord/Agent
        AND ac.date_closed_c IS NULL
        AND ac.date_won_c IS NULL
        AND op.stage_name IN ('Renewal','Fact Find','Meeting Booked','Negotiation','Agreement Preparation','Agreement Sent','Trial')

),

valid_leads AS (
-- Rebuilding query for https://unihomes.lightning.force.com/lightning/r/Report/00OUc0000069OyvMAE/view
 SELECT
        id AS salesforce_18_digit_id,
        company,
        associated_city_c AS associated_city,
        postal_code AS postcode,
        latitude,
        longitude,
        percentage_student_c AS total_student_portfolio,
        NULL AS account_type,
        'Prospect' AS category,
        'Lead' AS sf_object
    FROM
        salesforce.lead
    WHERE
        record_type_id = '012Uc000000Gue9IAC' AND  -- Need to pull in Record Type object to do this dynamically. Filters for Landlord/Agent
        main_contact_c = TRUE
        AND status <> 'Converted'
        AND status IN ('Unqualified this Season','New','Working','Nurture')

),

combined_data_table AS (
SELECT * FROM active_customers
UNION
SELECT * FROM active_opps
UNION
SELECT * FROM valid_leads
)

SELECT * FROM combined_data_table
