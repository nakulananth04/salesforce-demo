{{ 
  config(
    materialized='table'
  ) 
}}

WITH date_range AS (
    SELECT 
        MIN(TO_DATE(CREATED_DATE)) AS min_date,
        CURRENT_DATE() AS max_date
    FROM {{ ref('account') }}
),

date_spine AS (
    SELECT 
        DATEADD(DAY, SEQ4(), (SELECT min_date FROM date_range)) AS date_key
    FROM TABLE(GENERATOR(ROWCOUNT => 10000))  -- Adjust based on your actual range
),

filtered_dates AS (
    SELECT 
        date_key
    FROM date_spine, date_range
    WHERE date_key <= date_range.max_date
)

SELECT
    date_key AS date_key,
    YEAR(date_key) AS year,
    MONTH(date_key) AS month,
    DAY(date_key) AS day,
    DAYOFWEEK(date_key) AS day_of_week,
    DAYOFYEAR(date_key) AS day_of_year,
    QUARTER(date_key) AS quarter,
    WEEKOFYEAR(date_key) AS week_of_year
FROM filtered_dates
