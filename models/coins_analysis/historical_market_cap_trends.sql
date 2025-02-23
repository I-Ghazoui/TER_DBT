{{ config(materialized='incremental', unique_key='(id, creation_date)') }}

{% if is_incremental() %}
with max_date as (
    select coalesce(max(creation_date), '1970-01-01'::timestamp) as max_creation_date
    from {{ this }}
),
{% endif %}

base as (
    select *
    from {{ ref('transformed_coingecko_data_v') }}
    where 1=1
    {% if is_incremental() %}
        and creation_date > (select max_creation_date from max_date)
    {% endif %}
    and symbol in ('btc', 'eth', 'usdt', 'sol', 'xrp', 'doge', 'trx', 'ada', 'shib')
)

select
    cast(creation_date as date) as date,
    symbol,
    name,
    market_cap
from base
order by date asc