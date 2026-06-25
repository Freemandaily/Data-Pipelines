{{ config(
    materialized = 'table',
    alias = 'token_transfers',
    schema = 'facts'
) }} with transfers as (
    select *
    from {{ ref('int_token_transfers') }}
),
blocks as (
    select *
    from {{ ref('stg_blocks') }}
)
select t.log_id,
    t.transaction_hash,
    t.block_number,
    b.block_timestamp,
    t.from_address,
    t.to_address,
    t.token_address,
    t.raw_amount,
    t.symbol,
    t.decimals
from transfers t
    left join blocks b on t.block_number = b.block_number