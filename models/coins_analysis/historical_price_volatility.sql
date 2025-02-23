{{ config(materialized='table') }} -- ◀ Abandonner l'incrémental

SELECT
    symbol,
    name,
    AVG((high_24h - low_24h) / NULLIF(low_24h, 0) * 100) AS avg_volatility_percentage
FROM {{ ref('transformed_coingecko_data_v') }}
WHERE 1=1
    AND id IS NOT NULL
    AND id != ' '
    AND name IS NOT NULL
    AND name != ' '
    AND symbol IS NOT NULL
    AND symbol != ' '
GROUP BY symbol, name
ORDER BY avg_volatility_percentage DESC