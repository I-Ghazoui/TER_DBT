{{ config(materialized='incremental', unique_key='(id, creation_date)') }}

with base as (
    select *
    from {{ ref('transformed_coingecko_data_v') }}
    where 1=1
    {% if is_incremental() %}
       AND creation_date > (select max(creation_date) from {{ this }})
    {% endif %}
    AND symbol IN ('btc', 'eth', 'usdt', 'sol', 'xrp', 'doge', 'trx', 'ada', 'shib')
)

select
    cast(creation_date as date) as date,
    symbol,
    name,
    market_cap
from base
order by date asc