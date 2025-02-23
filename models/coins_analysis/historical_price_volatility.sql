{{ config(materialized='incremental', unique_key='(id, creation_date)') }}

with base as (
    select *
    from {{ ref('transformed_coingecko_data_v') }}
    where 1=1
    {% if is_incremental() %}
       AND creation_date > (select max(creation_date) from {{ this }})
    {% endif %}
    AND id is not null
    AND id != ' '
    AND name is not null
    AND name != ' '
    AND symbol is not null
    AND symbol != ' '
)

select
    symbol,
    name,
    avg((high_24h - low_24h) / nullif(low_24h, 0) * 100) as avg_volatility_percentage
from base
group by symbol, name
order by avg_volatility_percentage desc