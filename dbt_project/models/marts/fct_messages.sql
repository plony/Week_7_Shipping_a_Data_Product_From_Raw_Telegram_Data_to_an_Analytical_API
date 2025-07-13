SELECT
    m.message_id,
    c.channel_sk,
    d.date_sk,
    m.sender_id,
    m.message_content,
    m.views_count,
    m.forwards_count,
    m.replies_count,
    m.has_media,
    m.media_type,
    m.media_path,
    m.entities,
    LENGTH(m.message_content) AS message_length,
    -- Placeholder for enriched data (e.g., YOLO object detection results)
    NULL AS detected_objects,
    m.scraped_at
FROM {{ ref('stg_telegram_messages') }} m
JOIN {{ ref('dim_channels') }} c
    ON m.channel_name = c.channel_name
JOIN {{ ref('dim_dates') }} d
    ON m.message_timestamp::date = d.date_day