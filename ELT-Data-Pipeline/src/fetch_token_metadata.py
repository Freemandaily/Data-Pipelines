import os
import argparse
from web3 import Web3
from eth_abi import decode
from utils.config import get_db_connection, init_db

MULTICALL3_ADDRESS = "0xcA11bde05977b3631167028862bE2a173976CA11"
MULTICALL3_ABI = [
    {
        "inputs": [
            {
                "components": [
                    {"internalType": "address", "name": "target", "type": "address"},
                    {"internalType": "bool", "name": "allowFailure", "type": "bool"},
                    {"internalType": "bytes", "name": "callData", "type": "bytes"}
                ],
                "internalType": "struct Multicall3.Call3[]",
                "name": "calls",
                "type": "tuple[]"
            }
        ],
        "name": "aggregate3",
        "outputs": [
            {
                "components": [
                    {"internalType": "bool", "name": "success", "type": "bool"},
                    {"internalType": "bytes", "name": "returnData", "type": "bytes"}
                ],
                "internalType": "struct Multicall3.Result[]",
                "name": "returnArray",
                "type": "tuple[]"
            }
        ],
        "stateMutability": "payable",
        "type": "function"
    }
]

SIG_NAME = bytes.fromhex("06fdde03")
SIG_SYMBOL = bytes.fromhex("95d89b41")
SIG_DECIMALS = bytes.fromhex("313ce567")
SIG_TOTAL_SUPPLY = bytes.fromhex("18160ddd")

def decode_string(data: bytes) -> str | None:
    if len(data) == 0: return None
    try:
        return decode(['string'], data)[0]
    except:
        return data.split(b'\x00')[0].decode('utf-8', errors='ignore')

def decode_uint8(data: bytes) -> int | None:
    if len(data) == 0: return None
    try:
        val = decode(['uint8'], data)[0]
    except:
        val = int.from_bytes(data, byteorder='big')
    
    # PostgreSQL INTEGER max value is 2147483647
    if val > 2147483647:
        return None
    return val

def decode_uint256(data: bytes) -> int | None:
    if len(data) == 0: return None
    try:
        return decode(['uint256'], data)[0]
    except:
        return int.from_bytes(data, byteorder='big')

def sanitize(val: str | None) -> str | None:
    if val is None: return None
    if isinstance(val, str):
        return val.strip()
    return str(val).strip()

def multicall_fetch(token_addresses: list[str], w3: Web3, chunk_size: int = 500) -> list[dict]:
    multicall = w3.eth.contract(
        address=Web3.to_checksum_address(MULTICALL3_ADDRESS),
        abi=MULTICALL3_ABI
    )

    metadata = []
    total_tokens = len(token_addresses)
    
    for chunk_start in range(0, total_tokens, chunk_size):
        chunk_tokens = token_addresses[chunk_start:chunk_start + chunk_size]
        print(f"Fetching metadata for chunk {chunk_start} to {min(chunk_start + chunk_size, total_tokens)} of {total_tokens}...")
        
        calls = []
        for addr in chunk_tokens:
            target = Web3.to_checksum_address(addr)
            calls.append((target, True, SIG_NAME))
            calls.append((target, True, SIG_SYMBOL))
            calls.append((target, True, SIG_DECIMALS))
            calls.append((target, True, SIG_TOTAL_SUPPLY))

        raw = multicall.functions.aggregate3(calls).call()

        for i, addr in enumerate(chunk_tokens):
            base = i * 4

            name_ok,    name_data    = raw[base]
            symbol_ok,  symbol_data  = raw[base + 1]
            decimal_ok, decimal_data = raw[base + 2]
            supply_ok,  supply_data  = raw[base + 3]

            name     = sanitize(decode_string(name_data)    if name_ok    else None)
            symbol   = sanitize(decode_string(symbol_data)  if symbol_ok  else None)
            decimals = decode_uint8(decimal_data)            if decimal_ok else 18
            supply   = decode_uint256(supply_data)           if supply_ok  else None

            metadata.append({
                "token_address": addr.lower(),
                "token_name":    name,
                "symbol":        symbol,
                "decimals":      decimals,
                "total_supply":  str(supply) if supply is not None else None,
                "supply_source": "onchain"   if supply is not None else "derived",
            })

    return metadata


def fetch_token_metadata(rpc: str, start_block: int = None, end_block: int = None, tokens: list = None):
    init_db()
    
    w3 = Web3(Web3.HTTPProvider(rpc))
    if not w3.is_connected():
        print("Failed to connect to RPC")
        exit(1)

    tokens_to_fetch = set()
    if tokens:
        tokens_to_fetch.update([t.lower() for t in tokens])
        
    conn = get_db_connection()
    cursor = conn.cursor()
    
    if start_block is not None and end_block is not None:
        print(f"Querying database for tokens in blocks {start_block} to {end_block}...")
        cursor.execute("""
            SELECT DISTINCT address FROM logs 
            WHERE block_number >= %s AND block_number <= %s
            AND topics LIKE '["0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"%%'
        """, (start_block, end_block))
        
        db_tokens = [row[0] for row in cursor.fetchall() if row[0]]
        tokens_to_fetch.update([t.lower() for t in db_tokens])
        print(f"Found {len(db_tokens)} unique tokens from logs in block range.")
            
    if not tokens_to_fetch:
        print("No specific tokens or block range provided. Scanning entire logs table for missing token metadata...")
        cursor.execute("""
            SELECT DISTINCT address FROM logs 
            WHERE topics LIKE '["0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"%%'
        """)
        all_log_tokens = [row[0] for row in cursor.fetchall() if row[0]]
        tokens_to_fetch.update([t.lower() for t in all_log_tokens])
        print(f"Found {len(all_log_tokens)} total unique tokens in logs.")

    if not tokens_to_fetch:
        print("No tokens found in logs at all. Exiting.")
        cursor.close()
        conn.close()
        return

    # Get already existing tokens
    cursor.execute("SELECT token_address FROM token_metadata")
    existing_tokens = {row[0].lower() for row in cursor.fetchall()}
                    
    missing_tokens = [t for t in tokens_to_fetch if t not in existing_tokens]
    
    if not missing_tokens:
        print(f"All {len(tokens_to_fetch)} tokens already have metadata in the database. Nothing new to fetch.")
        cursor.close()
        conn.close()
        return
        
    print(f"Found {len(missing_tokens)} missing tokens. Fetching metadata via multicall...")
    new_data = multicall_fetch(missing_tokens, w3)

    if new_data:
        from psycopg2.extras import execute_values
        query = """
            INSERT INTO token_metadata (token_address, token_name, symbol, decimals, total_supply, supply_source)
            VALUES %s ON CONFLICT (token_address) DO NOTHING
        """
        values = [(
            d['token_address'], d['token_name'], d['symbol'], 
            d['decimals'], d['total_supply'], d['supply_source']
        ) for d in new_data]
        
        execute_values(cursor, query, values)
        conn.commit()
    
    cursor.close()
    conn.close()
    print(f"Successfully saved {len(new_data)} new token records to PostgreSQL.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Fetch ERC20 token metadata via Multicall3")
    parser.add_argument("--rpc", required=True, help="Ethereum RPC URL")
    parser.add_argument("--tokens", nargs="*", help="List of token addresses")
    parser.add_argument("--start-block", type=int, help="Extract tokens from logs data starting at this block")
    parser.add_argument("--end-block", type=int, help="Extract tokens from logs data ending at this block")
    args = parser.parse_args()

    fetch_token_metadata(rpc=args.rpc, start_block=args.start_block, end_block=args.end_block, tokens=args.tokens)