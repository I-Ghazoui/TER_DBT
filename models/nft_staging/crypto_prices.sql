SELECT
    SYMBOL,CURRENT_PRICE
FROM
    {{ ref('latest_transformed_coingecko_data_v') }}
WHERE SYMBOL IN ('ape', 'avax', 'bera', 'eth', 'flow', 'gala', 'magic', 'pol', 'sand', 'sei',
                   'sol', 'usdc', 'wavax', 'weth');