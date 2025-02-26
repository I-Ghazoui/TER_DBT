{{ config(
    materialized='table'
) }}

SELECT 
    event_type,
    chain,
    COUNT(*) AS event_count
FROM {{ ref('stg_nft_events') }}
GROUP BY event_type, chain
ORDER BY event_count DESC
