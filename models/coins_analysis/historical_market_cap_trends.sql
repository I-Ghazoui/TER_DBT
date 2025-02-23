{{ config(
    materialized='incremental',
    unique_key='(id, creation_date)'
) }}

{% if is_incremental() %}
    {% set max_date_query %}
        SELECT COALESCE(MAX(creation_date), '1970-01-01'::TIMESTAMP) 
        FROM {{ this }}
    {% endset %}
    {% set max_date = run_query(max_date_query).columns[0][0] %}
{% endif %}

SELECT
    CAST(creation_date AS DATE) AS date,
    symbol,
    name,
    market_cap
FROM {{ ref('transformed_coingecko_data_v') }}
WHERE 1=1
    AND symbol IN ('btc', 'eth', 'usdt', 'sol', 'xrp', 'doge', 'trx', 'ada', 'shib')
    {% if is_incremental() %}
        AND creation_date > '{{ max_date }}' -- Injection de la date pré-calculée
    {% endif %}
ORDER BY date ASC