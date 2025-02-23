{{
    config(
        partition_by={'field': 'updated_at', 'data_type': 'timestamp'},
        cluster_by=['nft_collection', 'nft_contract', 'CHAIN']
    )
}}

WITH metadata_cleanup AS (
    SELECT
        CHAIN,
        nft_identifier,
        nft_collection,
        nft_contract,
        nft_name,
        nft_description,
        nft_image_url,
        nft_opensea_url,
        CAST(nft_updated_at AS TIMESTAMP) AS updated_at,
        nft_is_disabled,
        nft_is_nsfw,
        nft_token_standard,
    FROM {{ ref('stg_nft_events_v') }} 
)

SELECT
    *,
    CASE
        WHEN nft_token_standard = 'erc721' THEN 'Single'
        WHEN nft_token_standard = 'erc1155' THEN 'Multi'
        WHEN nft_token_standard = 'erc20' THEN 'Fungible'
        WHEN nft_token_standard = 'erc777' THEN 'Fungible'
        WHEN nft_token_standard = 'erc998' THEN 'Composable'
        ELSE 'Other'
    END AS token_type
FROM metadata_cleanup
WHERE NOT ARRAY_CONTAINS(NULL, ARRAY_CONSTRUCT(
    CHAIN,
    nft_identifier,
    nft_collection,
    nft_contract,
    nft_name,
    nft_description,
    nft_image_url,
    nft_opensea_url,
    updated_at,
    nft_is_disabled,
    nft_is_nsfw,
    nft_token_standard
))

{% if is_incremental() %}
WHERE updated_at > (SELECT MAX(updated_at) FROM {{ this }})
{% endif %}