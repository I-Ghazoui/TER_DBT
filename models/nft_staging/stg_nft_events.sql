{{ config(
    materialized='view'
) }}

WITH nft_events_cleaned AS (
    SELECT 
        EVENT_TYPE::VARCHAR AS event_type,
        CHAIN::VARCHAR AS chain,
        TRANSACTION::VARCHAR AS transaction_hash,
        QUANTITY::NUMBER AS nft_quantity,

        -- Convert epoch to timestamp
        TO_TIMESTAMP(EVENT_TIMESTAMP) AS event_timestamp,
        NFT_IDENTIFIER::VARCHAR AS nft_id,
        TRIM(NFT_COLLECTION)::VARCHAR AS collection_name, -- Clean whitespace
        NFT_CONTRACT::VARCHAR AS contract_address,
        NFT_NAME::VARCHAR AS nft_name,
        NFT_IMAGE_URL::VARCHAR AS image_url,
        NFT_OPENSEA_URL::VARCHAR AS opensea_url,
        NFT_TOKEN_STANDARD::VARCHAR AS token_standard,
        NFT_UPDATED_AT::TIMESTAMP AS updated_at,
        NFT_DESCRIPTION::VARCHAR AS description,
        NFT_IS_DISABLED::BOOLEAN AS is_disabled,
        NFT_IS_NSFW::BOOLEAN AS is_nsfw,
        SELLER::VARCHAR AS seller_address,
        BUYER::VARCHAR AS buyer_address,

        -- Adjust payment quantity for decimals (e.g., ETH has 18 decimals)
        (PAYMENT_QUANTITY / POWER(10, PAYMENT_DECIMALS))::FLOAT AS price,
        PAYMENT_SYMBOL::VARCHAR AS currency,
        PAYMENT_TOKEN_ADDRESS::VARCHAR AS payment_token_address,
        FROM_ADDRESS::VARCHAR AS from_address,
        TO_ADDRESS::VARCHAR AS to_address,
        ORDER_HASH::VARCHAR AS order_hash,
        PROTOCOL_ADDRESS::VARCHAR AS protocol_address,
        TO_TIMESTAMP(CLOSING_DATE) AS closing_date

    FROM TER_DATABASE.TER_RAW_DATA.NFT_EVENTS
    WHERE EVENT_TIMESTAMP IS NOT NULL
)

SELECT *
FROM nft_events_cleaned
