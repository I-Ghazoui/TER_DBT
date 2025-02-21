{{ config(materialized='incremental', unique_key='(id, creation_date)') }}

with base as (
    select *
    from {{ ref('transformed_coingecko_data_v') }}
    {% if is_incremental() %}
       where creation_date > (select max(creation_date) from {{ this }})
            AND WHERE symbol IN ('btc', 'eth', 'usdt', 'sol', 'xrp', 'doge', 'trx', 'ada', 'shib')
    {% endif %}
)

select
    cast(creation_date as date) as date,
    symbol,
    name,
    market_cap
from base
order by date asc
