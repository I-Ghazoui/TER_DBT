WITH price_trends AS (
    SELECT 
        CRYPTO_ID,
        OHLC_TIMESTAMP::DATE AS TRADE_DATE, -- Conversion de OHLC_TIMESTAMP en DATE
        CLOSE_PRICE,
        AVG(CLOSE_PRICE) OVER (PARTITION BY CRYPTO_ID ORDER BY OHLC_TIMESTAMP ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS MA_7D,
        AVG(CLOSE_PRICE) OVER (PARTITION BY CRYPTO_ID ORDER BY OHLC_TIMESTAMP ROWS BETWEEN 29 PRECEDING AND CURRENT ROW) AS MA_30D
    FROM TER_DATABASE.TER_RAW_DATA.COINGECKO_OHLC_DATA
    WHERE CLOSE_PRICE IS NOT NULL -- Exclure les lignes avec des prix manquants
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
        ON p.CRYPTO_ID = c.ID -- Jointure sur CRYPTO_ID
    WHERE c.SYMBOL IS NOT NULL -- Exclure les symboles invalides
      AND c.NAME IS NOT NULL -- Exclure les noms invalides
)
SELECT *
FROM trend_analysis
WHERE TREND_STATUS = 'Uptrend' -- Focus on uptrends
ORDER BY TRADE_DATE DESC
LIMIT 100 -- Limite les résultats pour éviter des volumes excessifs