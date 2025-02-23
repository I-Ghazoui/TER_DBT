{{ config(
    materialized='incremental',
    unique_key='event_hash',
    partition_by={'field': 'event_timestamp', 'data_type': 'timestamp'},
    cluster_by=['nft_contract']
) }}

WITH base AS (
    SELECT
        TRANSACTION AS event_hash,
        EVENT_TYPE,
        CHAIN,
        CAST(EVENT_TIMESTAMP AS TIMESTAMP_NTZ) AS event_timestamp,
        NFT_IDENTIFIER,
        NFT_COLLECTION,
        NFT_CONTRACT,
        QUANTITY,
        COALESCE(
            TRY_CAST(PAYMENT_QUANTITY AS DECIMAL(38, 0)) / POWER(10, PAYMENT_DECIMALS),
            0
        ) AS sale_amount,
        PAYMENT_SYMBOL AS currency,
        SELLER,
        BUYER,
        FROM_ADDRESS,
        TO_ADDRESS,
        CAST(CLOSING_DATE AS TIMESTAMP_NTZ) AS closing_date,
    FROM TER_DATABASE.TER_RAW_DATA.NFT_EVENTS
    WHERE EVENT_TYPE IN ('sale', 'transfer')
)

SELECT *
FROM base
WHERE NOT ARRAY_CONTAINS(NULL, ARRAY_CONSTRUCT(
    event_hash,
    event_timestamp,
    EVENT_TYPE,
    CHAIN,
    NFT_IDENTIFIER,
    NFT_COLLECTION,
    NFT_CONTRACT,
    QUANTITY,
    sale_amount,
    currency,
    SELLER,
    BUYER,
    FROM_ADDRESS,
    TO_ADDRESS,
    closing_date
))

{% if is_incremental() %}
WHERE event_timestamp > (SELECT MAX(event_timestamp) FROM {{ this }})
{% endif %}
