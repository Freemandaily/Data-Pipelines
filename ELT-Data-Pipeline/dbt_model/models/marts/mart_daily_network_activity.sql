{{ config(
    materialized = 'table',
    alias = 'daily_network_activity',
    schema = 'facts'
) }} with blocks as (
    select *
    from {{ ref('stg_blocks') }}
),
transactions as (
    select *
    from {{ ref('stg_transactions') }}
)
select date_trunc('day', b.block_timestamp) as activity_date,
    count(distinct b.block_number) as total_blocks,
    count(distinct t.transaction_hash) as total_transactions,
    count(distinct t.from_address) as unique_active_users,
    sum(t.gas_provided) as total_gas_provided
from blocks b
    left join transactions t on b.block_number = t.block_number
group by 1