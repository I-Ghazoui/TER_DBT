WITH sales_data AS (
    SELECT
        event_hash AS sale_id,
        event_timestamp,
        closing_date,
        CHAIN,
        nft_collection,
        nft_contract,
        nft_identifier,
        nft_name,
        nft_description,
        nft_image_url,
        updated_at AS nft_updated_at,
        nft_is_disabled,
        nft_is_nsfw,
        nft_token_standard,
        seller,
        buyer,
        sale_amount,
        CRYPTO_SYMBOL,
        sale_price,
        sale_category,
        COALESCE(TRY_CAST(quantity AS INTEGER), 1) AS quantity,
        COUNT(*) OVER (PARTITION BY nft_identifier, nft_collection) AS nft_total_sales,
        RANK() OVER (PARTITION BY nft_collection ORDER BY sale_price DESC) AS nft_price_rank,
        LAG(sale_price) OVER (PARTITION BY nft_identifier, nft_collection ORDER BY event_timestamp) AS nft_previous_sale_price,
        sale_price - LAG(sale_price) OVER (PARTITION BY nft_identifier, nft_collection ORDER BY event_timestamp) AS profit_or_loss
    FROM {{ ref('stg_events') }}
    WHERE EVENT_TYPE = 'sale'
)

SELECT
    *,
    CASE
        WHEN nft_previous_sale_price IS NULL OR nft_previous_sale_price = 0 THEN 0
        ELSE ((sale_price - nft_previous_sale_price) / nft_previous_sale_price) * 100
    END AS price_change_percent,
        CASE
        WHEN nft_token_standard = 'erc721' THEN 'Single'
        WHEN nft_token_standard = 'erc1155' THEN 'Multi'
        ELSE 'Other'
    END AS token_type,
    CASE
        WHEN profit_or_loss > 1000 THEN 'High Profit'
        WHEN profit_or_loss BETWEEN 1 AND 999 THEN 'Low Profit'
        WHEN profit_or_loss = 0 THEN 'Neutral'
        ELSE 'Loss'
    END AS sale_profit_category
FROM sales_data
