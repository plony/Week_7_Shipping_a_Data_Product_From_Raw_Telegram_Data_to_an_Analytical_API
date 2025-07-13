WITH raw_messages AS (
    SELECT
        id,
        sender_id,
        date,
        message,
        views,
        forwards,
        replies_count,
        has_media,
        media_type,
        media_path,
        entities,
        channel_name,
        scraped_at
    FROM {{ source('raw_data', 'raw_telegram_messages') }}
)

SELECT
    id AS message_id,
    sender_id,
    CAST(date AS TIMESTAMP) AS message_timestamp,
    message AS message_content,
    COALESCE(views, 0) AS views_count,
    COALESCE(forwards, 0) AS forwards_count,
    COALESCE(replies_count, 0) AS replies_count,
    has_media,
    media_type,
    media_path,
    entities,
    channel_name,
    scraped_at
FROM raw_messages