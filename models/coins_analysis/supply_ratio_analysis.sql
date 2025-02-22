WITH supply_analysis AS (
    SELECT 
        SYMBOL,
        NAME,
        CIRCULATING_SUPPLY,
        TOTAL_SUPPLY,
        MAX_SUPPLY,
        (CIRCULATING_SUPPLY / NULLIF(TOTAL_SUPPLY, 0)) * 100 AS SUPPLY_RATIO_PERCENTAGE
    FROM {{ ref('transformed_coingecko_data_v') }}
    WHERE 1=1
    AND ID IS NOT NULL
    AND ID != ' '
    AND NAME IS NOT NULL
    AND NAME != ' '
    AND SYMBOL IS NOT NULL
    AND SYMBOL != ' '
    AND SYMBOL NOT IN ('usdt', 'usdc', 'usds', 'wbtc', 'steth') -- Exclude stablecoins
    
)

SELECT 
    SYMBOL,
    NAME,
    CIRCULATING_SUPPLY,
    TOTAL_SUPPLY,
    MAX_SUPPLY,
    SUPPLY_RATIO_PERCENTAGE
FROM supply_analysis
ORDER BY SUPPLY_RATIO_PERCENTAGE DESC

-- bar chat, we can visulaize also see the "rare" ones that can be affected by its inflation.