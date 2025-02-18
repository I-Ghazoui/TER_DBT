WITH coin_market_efficiency AS (
    SELECT
        symbol,
        coin_name,
        market_cap,
        total_volume,
        (total_volume / market_cap) * 100 AS efficiency_ratio,
        creation_date
    FROM {{ ref('transformed_coingecko_data') }}
)

SELECT *
FROM coin_market_efficiency
ORDER BY efficiency_ratio DESC
