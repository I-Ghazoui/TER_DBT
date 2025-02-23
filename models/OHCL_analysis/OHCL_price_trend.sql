WITH top_20_cryptos AS (
    -- Sélectionner les 20 premières cryptomonnaies selon leur rang de capitalisation boursière
    -- Exclure les stablecoins
    SELECT 
        ID,
        SYMBOL,
        NAME,
        MARKET_CAP_RANK
    FROM {{ ref('transformed_coingecko_data_v') }}
    WHERE MARKET_CAP_RANK <= 20
      AND SYMBOL NOT IN ('USDT', 'USDC', 'BUSD', 'DAI', 'USTC', 'FRAX', 'TUSD', 'USDP', 'USDD', 'GUSD', 'EURT', 'MIM', 'ALUSD', 'XAUT', 'LUSD', 'RAI', 'UXD', 'EUROC', 'CBDO', 'CUSD') -- Liste des stablecoins à exclure
),
daily_averages AS (
    -- Calculer les moyennes quotidiennes pour chaque cryptomonnaie
    SELECT 
        c.CRYPTO_ID,
        c.OHLC_TIMESTAMP::DATE AS TRADE_DATE,
        AVG(c.OPEN_PRICE) AS AVG_OPEN_PRICE,
        AVG(c.HIGH_PRICE) AS AVG_HIGH_PRICE,
        AVG(c.LOW_PRICE) AS AVG_LOW_PRICE,
        AVG(c.CLOSE_PRICE) AS AVG_CLOSE_PRICE
    FROM TER_DATABASE.TER_RAW_DATA.COINGECKO_OHLC_DATA c
    JOIN top_20_cryptos t20
        ON c.CRYPTO_ID = t20.ID
    GROUP BY c.CRYPTO_ID, c.OHLC_TIMESTAMP::DATE
),
price_trends AS (
    -- Calculer les moyennes mobiles sur 7 jours et 30 jours
    SELECT 
        CRYPTO_ID,
        TRADE_DATE,
        AVG_CLOSE_PRICE,
        AVG(AVG_CLOSE_PRICE) OVER (PARTITION BY CRYPTO_ID ORDER BY TRADE_DATE ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS MA_7D,
        AVG(AVG_CLOSE_PRICE) OVER (PARTITION BY CRYPTO_ID ORDER BY TRADE_DATE ROWS BETWEEN 29 PRECEDING AND CURRENT ROW) AS MA_30D
    FROM daily_averages
),
trend_analysis AS (
    -- Analyser les tendances (Uptrend, Downtrend, Neutral)
    SELECT 
        p.CRYPTO_ID,
        t20.SYMBOL,
        p.TRADE_DATE,
        p.AVG_CLOSE_PRICE,
        p.MA_7D,
        p.MA_30D,
        CASE 
            WHEN p.MA_7D > p.MA_30D THEN 'Uptrend'
            WHEN p.MA_7D < p.MA_30D THEN 'Downtrend'
            ELSE 'Neutral'
        END AS TREND_STATUS
    FROM price_trends p
    JOIN top_20_cryptos t20
        ON p.CRYPTO_ID = t20.ID
)
-- Résultat final : afficher les données pour les 20 premières cryptomonnaies (sans stablecoins)
SELECT 
    SYMBOL,
    TRADE_DATE,
    AVG_CLOSE_PRICE,
    MA_7D,
    MA_30D,
    TREND_STATUS
FROM trend_analysis
ORDER BY SYMBOL ASC, TRADE_DATE DESC