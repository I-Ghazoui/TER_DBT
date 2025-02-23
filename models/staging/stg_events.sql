{{ config(
    materialized='incremental',
    unique_key='event_hash',
    partition_by={'field': 'event_timestamp', 'data_type': 'timestamp'},
    cluster_by=['nft_collection']
) }}

WITH crypto_prices AS (
    SELECT
        SYMBOL AS crypto_symbol,
        CURRENT_PRICE AS price
    FROM {{ ref('latest_transformed_coingecko_data_v') }}
),

base AS (
    SELECT
        TRANSACTION AS event_hash,
        EVENT_TYPE,
        CHAIN,
        CAST(EVENT_TIMESTAMP AS TIMESTAMP_NTZ) AS event_timestamp,
        NFT_IDENTIFIER,
        NFT_COLLECTION,
        NFT_CONTRACT,
        nft_name,
        nft_description,
        nft_image_url,
        nft_opensea_url,
        CAST(nft_updated_at AS TIMESTAMP) AS updated_at,
        nft_is_disabled,
        nft_is_nsfw,
        nft_token_standard,
        QUANTITY,
        COALESCE(
            TRY_CAST(PAYMENT_QUANTITY AS DECIMAL(38, 0)) / POWER(10, PAYMENT_DECIMALS),
            0
        ) AS sale_amount,
        CASE
            WHEN LOWER(PAYMENT_SYMBOL) = 'wape' THEN 'ape'
            WHEN LOWER(PAYMENT_SYMBOL) = 'wbera' THEN 'bera'
            WHEN LOWER(PAYMENT_SYMBOL) = 'wflow' THEN 'flow'
            WHEN LOWER(PAYMENT_SYMBOL) = 'wsei' THEN 'sei'
            WHEN LOWER(PAYMENT_SYMBOL) = 'klay' THEN 'sei'
            ELSE LOWER(PAYMENT_SYMBOL)
        END AS CRYPTO_SYMBOL,
        SELLER,
        BUYER,
        FROM_ADDRESS,
        TO_ADDRESS,
        CAST(CLOSING_DATE AS TIMESTAMP_NTZ) AS closing_date,
    FROM TER_DATABASE.TER_RAW_DATA.NFT_EVENTS
    WHERE EVENT_TYPE IN ('sale', 'transfer')
)

SELECT
    base.*,
    COALESCE(crypto_prices.price, 0) AS price,
    base.sale_amount * COALESCE(crypto_prices.price, 0) AS sale_price
FROM base
LEFT JOIN crypto_prices
ON base.CRYPTO_SYMBOL = crypto_prices.crypto_symbol

{% if is_incremental() %}
WHERE event_timestamp > (SELECT MAX(event_timestamp) FROM {{ this }})
{% endif %}
