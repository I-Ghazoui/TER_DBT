{{ config(materialized='table') }} -- ◀ Changement en table standard

WITH base AS (
    SELECT 
        id,
        symbol,
        name,
        current_price,
        high_24h,
        low_24h,
        ath,
        atl,
        creation_date::DATE AS trade_date
    FROM {{ ref('transformed_coingecko_data_v') }}
    WHERE 
        id IS NOT NULL
        AND id != ' '
        AND name IS NOT NULL
        AND name != ' '
        AND symbol IS NOT NULL
        AND symbol != ' '
        AND symbol NOT IN ('usdt', 'usdc', 'usds', 'wbtc', 'steth')
),

volatility_analysis AS (
    SELECT 
        symbol,
        name,
        (high_24h - low_24h) / NULLIF(current_price, 0) * 100 AS relative_volatility_percentage,
        (current_price - ath) / NULLIF(ath, 0) * 100 AS distance_to_ath_percentage,
        (current_price - atl) / NULLIF(atl, 0) * 100 AS distance_to_atl_percentage
    FROM base
    WHERE atl > 0.0001
),

aggregated_metrics AS (
    SELECT 
        symbol,
        name,
        AVG(relative_volatility_percentage) AS avg_relative_volatility,
        AVG(distance_to_ath_percentage) AS avg_distance_to_ath,
        AVG(distance_to_atl_percentage) AS avg_distance_to_atl
    FROM volatility_analysis
    GROUP BY symbol, name
)

SELECT 
    symbol,
    name,
    avg_relative_volatility,
    avg_distance_to_ath,
    avg_distance_to_atl
FROM aggregated_metrics
WHERE avg_distance_to_atl < 15000
ORDER BY avg_relative_volatility DESC
LIMIT 20