WITH transfer_data AS (
    SELECT
        event_hash AS transfer_id,
        event_timestamp,
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
        from_address,
        to_address,
        COALESCE(TRY_CAST(quantity AS INTEGER), 1) AS quantity,
        COUNT(*) OVER (PARTITION BY nft_identifier, nft_collection) AS total_transfers,
        RANK() OVER (PARTITION BY nft_collection ORDER BY event_timestamp) AS transfer_rank,
        LAG(to_address) OVER (PARTITION BY nft_identifier, nft_collection ORDER BY event_timestamp) AS prev_owner,
    FROM {{ ref('stg_events') }}
    WHERE event_type = 'transfer'
)

SELECT
    *,
    CASE
        WHEN from_address = '0x0000000000000000000000000000000000000000' THEN 'Mint'
        WHEN to_address = '0x0000000000000000000000000000000000000000' THEN 'Burn'
        ELSE 'Transfer'
    END AS transfer_type
FROM transfer_data
