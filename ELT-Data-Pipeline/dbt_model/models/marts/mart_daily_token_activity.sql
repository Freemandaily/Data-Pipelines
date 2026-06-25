{{ config(
    materialized = 'table',
    alias = 'daily_token_activity',
    schema = 'facts'
) }} with token_transfers as (
    select *
    from {{ ref('fct_token_transfers') }}
)
select date_trunc('day', block_timestamp) as activity_date,
    token_address,
    symbol,
    count(distinct transaction_hash) as total_transfers,
    count(distinct from_address) as unique_senders,
    count(distinct to_address) as unique_receivers
from token_transfers
group by 1,
    2,
    3