CREATE OR REPLACE VIEW TER_DATABASE.TER_ANALYSIS_DATA.OHCL_PRICE_TREND (
    CRYPTO_ID,
    SYMBOL,
    NAME,
    TRADE_DATE,
    CLOSE_PRICE,
    MA_7D,
    MA_30D,
    TREND_STATUS
) AS
WITH filtered_cryptos AS (
    -- Filtrer uniquement les cryptomonnaies spécifiées
    SELECT 
        ID AS CRYPTO_ID,
        SYMBOL,
        NAME
    FROM {{ ref('transformed_coingecko_data_v') }}
    WHERE SYMBOL IN ('btc', 'eth', 'xrp', 'bnb', 'sol', 'doge', 'ada', 'trx', 'link', 'avax', 'sui', 'xlm', 'litecoin', 'ton', 'shib', 'leo', 'om', 'hype', 'dot', 'uni', 'xmr', 'near', 'pepe', 'apt', 'dai', 'icp')
),
daily_averages AS (
    -- Calculer les moyennes quotidiennes pour chaque cryptomonnaie
    SELECT 
        c.CRYPTO_ID,
        c.OHLC_TIMESTAMP::DATE AS TRADE_DATE,
        AVG(c.CLOSE_PRICE) AS CLOSE_PRICE
    FROM TER_DATABASE.TER_RAW_DATA.COINGECKO_OHLC_DATA c
    JOIN filtered_cryptos f
        ON c.CRYPTO_ID = f.CRYPTO_ID
    GROUP BY c.CRYPTO_ID, c.OHLC_TIMESTAMP::DATE
),
price_trends AS (
    -- Calculer les moyennes mobiles sur 7 jours et 30 jours
    SELECT 
        CRYPTO_ID,
        TRADE_DATE,
        CLOSE_PRICE,
        AVG(CLOSE_PRICE) OVER (PARTITION BY CRYPTO_ID ORDER BY TRADE_DATE ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS MA_7D,
        AVG(CLOSE_PRICE) OVER (PARTITION BY CRYPTO_ID ORDER BY TRADE_DATE ROWS BETWEEN 29 PRECEDING AND CURRENT ROW) AS MA_30D
    FROM daily_averages
),
trend_analysis AS (
    -- Analyser les tendances (Uptrend, Downtrend, Neutral)
    SELECT 
        p.CRYPTO_ID,
        f.SYMBOL,
        f.NAME,
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
    JOIN filtered_cryptos f
        ON p.CRYPTO_ID = f.CRYPTO_ID
)
-- Résultat final : sélectionner les données pour la vue
SELECT 
    CRYPTO_ID,
    SYMBOL,
    NAME,
    TRADE_DATE,
    CLOSE_PRICE,
    MA_7D,
    MA_30D,
    TREND_STATUS
FROM trend_analysis
ORDER BY SYMBOL ASC, TRADE_DATE DESC