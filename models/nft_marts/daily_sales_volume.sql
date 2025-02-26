{{ config(
    materialized='table'
) }}

WITH sales_data AS (
    SELECT 
        DATE_TRUNC('DAY', event_timestamp) AS date,
        SUM(price) AS total_sales,
        COUNT(transaction_hash) AS total_transactions
    FROM {{ ref('stg_nft_events') }}
    WHERE event_type = 'sale'
    GROUP BY 1
)

SELECT *
FROM sales_data
