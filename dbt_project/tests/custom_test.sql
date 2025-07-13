-- models/marts/tests/positive_views_count.sql
-- This test checks if the views_count in fct_messages is always non-negative.
SELECT
    message_id
FROM {{ ref('fct_messages') }}
WHERE views_count < 0