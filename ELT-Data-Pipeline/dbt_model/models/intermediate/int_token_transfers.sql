with logs as (
    select * from {{ ref('stg_logs') }}
),
tokens as (
    select * from {{ ref('stg_token_metadata') }}
),
transfers as (
    select
        log_id,
        transaction_hash,
        block_number,
        log_index,
        contract_address as token_address,
        topics::json->>1 as from_address_raw,
        topics::json->>2 as to_address_raw,
        data
    from logs
    where topics like '["0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"%%'
)

select
    t.log_id,
    t.transaction_hash,
    t.block_number,
    t.log_index,
    t.token_address,
    '0x' || right(t.from_address_raw, 40) as from_address,
    '0x' || right(t.to_address_raw, 40) as to_address,
    t.data as raw_amount,
    tok.symbol,
    tok.decimals
from transfers t
left join tokens tok on t.token_address = tok.token_address
