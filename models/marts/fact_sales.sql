{{
    config(
        partition_by={'field': 'event_timestamp', 'data_type': 'timestamp'},
        cluster_by=['nft_contract']
    )
}}

WITH sales_data AS (
    SELECT
        event_hash AS sale_id,
        event_timestamp,
        closing_date,
        CHAIN,
        nft_contract,
        nft_identifier,
        seller,
        buyer,
        sale_amount,
        currency,
        COALESCE(TRY_CAST(quantity AS INTEGER), 1) AS quantity,
        COUNT(*) OVER (PARTITION BY nft_identifier, nft_contract) AS total_sales,
        RANK() OVER (PARTITION BY nft_identifier, nft_contract ORDER BY sale_amount DESC) AS price_rank,
        LAG(sale_amount) OVER (PARTITION BY nft_identifier, nft_contract ORDER BY event_timestamp) AS previous_sale_price
    FROM {{ ref('stg_events') }}
    WHERE EVENT_TYPE = 'sale'
)

SELECT
    *,
    CASE
        WHEN previous_sale_price IS NULL OR previous_sale_price = 0 THEN 0
        ELSE ((sale_amount - previous_sale_price) / previous_sale_price) * 100
    END AS price_change_percent
FROM sales_data
