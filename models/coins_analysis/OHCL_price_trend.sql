WITH price_trends AS (
    SELECT 
        CRYPTO_ID,
        OHLC_TIMESTAMP::DATE AS TRADE_DATE,
        CLOSE_PRICE,
        AVG(CLOSE_PRICE) OVER (PARTITION BY CRYPTO_ID ORDER BY OHLC_TIMESTAMP ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS MA_7D,
        AVG(CLOSE_PRICE) OVER (PARTITION BY CRYPTO_ID ORDER BY OHLC_TIMESTAMP ROWS BETWEEN 29 PRECEDING AND CURRENT ROW) AS MA_30D
    FROM {{ ref('transformed_coingecko_data_v') }}
),

trend_analysis AS (
    SELECT 
        p.CRYPTO_ID,
        c.SYMBOL,
        c.NAME,
        p.TRADE_DATE,
        p.CLOSE_PRICE,
        p.MA_7D,
        p.MA_30D,
        CASE 
            WHEN p.MA_7D > p.MA_30D THEN 'Uptrend'
            WHEN p.MA_7D < p.MA_30D THEN 'Downtrend'
            ELSE 'Neutral'
        END AS TREND_STATUS
    FROM price_trends p
    JOIN {{ ref('transformed_coingecko_data_v') }} c
        ON p.CRYPTO_ID = c.ID
),

aggregated_trends AS (
    SELECT 
        SYMBOL,
        NAME,
        SUM(CASE WHEN TREND_STATUS = 'Uptrend' THEN 1 ELSE 0 END) AS UPTREND_COUNT,
        SUM(CASE WHEN TREND_STATUS = 'Downtrend' THEN 1 ELSE 0 END) AS DOWNTREND_COUNT,
        (SUM(CASE WHEN TREND_STATUS = 'Uptrend' THEN 1 ELSE 0 END) - SUM(CASE WHEN TREND_STATUS = 'Downtrend' THEN 1 ELSE 0 END)) AS NET_TREND_SCORE
    FROM trend_analysis
    GROUP BY SYMBOL, NAME
)

SELECT 
    SYMBOL,
    NAME,
    UPTREND_COUNT,
    DOWNTREND_COUNT,
    NET_TREND_SCORE
FROM aggregated_trends
ORDER BY NET_TREND_SCORE DESC -- Classement par score net de tendance
LIMIT 20 -- Sélectionner les 20 meilleures cryptomonnaies