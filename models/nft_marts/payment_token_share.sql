{{ config(
    materialized='table'
) }}

WITH currency_sales AS (
    SELECT 
        currency,
        SUM(price) AS total_sales
    FROM {{ ref('stg_nft_events') }}
    WHERE currency IS NOT NULL
    GROUP BY currency
),

total_sales_amount AS (
    SELECT SUM(total_sales) AS grand_total
    FROM currency_sales
)

SELECT 
    cs.currency,
    cs.total_sales,
    (cs.total_sales / t.grand_total) * 100 AS percentage
FROM currency_sales cs
CROSS JOIN total_sales_amount t
ORDER BY cs.total_sales DESC
