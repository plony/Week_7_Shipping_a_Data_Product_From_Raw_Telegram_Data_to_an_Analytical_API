WITH date_spine AS (
    SELECT
        date::date AS date_day,
        EXTRACT(YEAR FROM date) AS year,
        EXTRACT(MONTH FROM date) AS month,
        EXTRACT(DAY FROM date) AS day,
        EXTRACT(DOY FROM date) AS day_of_year,
        EXTRACT(WEEK FROM date) AS week_of_year,
        EXTRACT(DOW FROM date) AS day_of_week, -- 0 for Sunday, 6 for Saturday
        TO_CHAR(date, 'Day') AS day_name,
        TO_CHAR(date, 'Month') AS month_name,
        CAST(date AS DATE) = CURRENT_DATE AS is_current_day,
        CAST(date AS DATE) = (CURRENT_DATE - INTERVAL '1 day') AS is_yesterday,
        CASE WHEN EXTRACT(DOW FROM date) IN (0, 6) THEN TRUE ELSE FALSE END AS is_weekend
    FROM (
        SELECT GENERATE_SERIES(
            (SELECT MIN(message_timestamp) FROM {{ ref('stg_telegram_messages') }})::date,
            (SELECT MAX(message_timestamp) FROM {{ ref('stg_telegram_messages') }})::date + INTERVAL '1 day',
            '1 day'::interval
        ) AS date
    ) AS dates
)

SELECT
    {{ dbt_utils.surrogate_key(['date_day']) }} AS date_sk,
    date_day,
    year,
    month,
    day,
    day_of_year,
    week_of_year,
    day_of_week,
    day_name,
    month_name,
    is_current_day,
    is_yesterday,
    is_weekend
FROM date_spine