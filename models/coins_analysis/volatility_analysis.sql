{{ config(materialized='incremental', unique_key='(id, creation_date)') }}

WITH base AS (
    SELECT 
        SYMBOL,
        NAME,
        CURRENT_PRICE,
        HIGH_24H,
        LOW_24H,
        ATH,
        ATL,
        CREATION_DATE::DATE AS TRADE_DATE
    FROM {{ ref('transformed_coingecko_data_v') }}
    WHERE 1=1
    {% if is_incremental() %}
       AND CREATION_DATE > (SELECT MAX(CREATION_DATE) FROM {{ this }})
    {% endif %}
    AND ID IS NOT NULL
    AND ID != ' '
    AND NAME IS NOT NULL
    AND NAME != ' '
    AND SYMBOL IS NOT NULL
    AND SYMBOL != ' '
    AND SYMBOL NOT IN ('usdt', 'usdc', 'usds', 'wbtc', 'steth')
    AND ATL > 0.0001 -- Exclure les cryptomonnaies avec un ATL extrêmement bas
),
volatility_analysis AS (
    SELECT 
        SYMBOL,
        NAME,
        TRADE_DATE,
        (HIGH_24H - LOW_24H) / NULLIF(CURRENT_PRICE, 0) * 100 AS RELATIVE_VOLATILITY_PERCENTAGE,
        (CURRENT_PRICE - ATH) / NULLIF(ATH, 0) * 100 AS DISTANCE_TO_ATH_PERCENTAGE,
        (CURRENT_PRICE - ATL) / NULLIF(ATL, 0) * 100 AS DISTANCE_TO_ATL_PERCENTAGE,
        CASE 
            WHEN (CURRENT_PRICE - ATH) / NULLIF(ATH, 0) * 100 > -20 THEN 'Proche ATH'
            WHEN (CURRENT_PRICE - ATH) / NULLIF(ATH, 0) * 100 BETWEEN -50 AND -20 THEN 'Moyennement Proche ATH'
            ELSE 'Loin ATH'
        END AS ATH_PROXIMITY_CATEGORY
    FROM base
)
SELECT 
    SYMBOL,
    NAME,
    TRADE_DATE,
    AVG(RELATIVE_VOLATILITY_PERCENTAGE) AS AVG_RELATIVE_VOLATILITY,
    AVG(DISTANCE_TO_ATH_PERCENTAGE) AS AVG_DISTANCE_TO_ATH,
    AVG(DISTANCE_TO_ATL_PERCENTAGE) AS AVG_DISTANCE_TO_ATL,
    MAX(ATH_PROXIMITY_CATEGORY) AS ATH_PROXIMITY_CATEGORY
FROM volatility_analysis
GROUP BY SYMBOL, NAME, TRADE_DATE
ORDER BY TRADE_DATE DESC, AVG_RELATIVE_VOLATILITY DESC