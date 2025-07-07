

SELECT
REGEXP_SUBSTR(postcode,'[A-Z]+') AS city_proxy,
SUM(CASE WHEN category = 'Customer' THEN 1 WHEN total_student_portfolio >=5 THEN 1 ELSE 0 END) AS Student_Letting_Agents,
SUM(CASE WHEN category = 'Customer' THEN 1 ELSE 0 END) AS Clients,
SUM(CASE WHEN category = 'Customer' THEN 1 WHEN total_student_portfolio >=5 THEN total_student_portfolio ELSE 0 END) AS Student_Portfolio,
SUM(CASE WHEN category = 'Customer' THEN total_student_portfolio ELSE 0 END) AS Clients_Student_Portfolio

FROM {{ ref ('marketshare_dataset_test') }}
GROUP BY 1
ORDER BY student_portfolio DESC
