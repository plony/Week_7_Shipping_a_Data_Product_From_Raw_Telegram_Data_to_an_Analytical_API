SELECT DISTINCT
    {{ dbt_utils.surrogate_key(['channel_name']) }} AS channel_sk,
    channel_name
FROM {{ ref('stg_telegram_messages') }}
WHERE channel_name IS NOT NULL