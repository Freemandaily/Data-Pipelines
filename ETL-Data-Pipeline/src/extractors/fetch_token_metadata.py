import os
import csv
import argparse
from web3 import Web3
from eth_abi import decode

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
        return decode(['uint8'], data)[0]
    except:
        return int.from_bytes(data, byteorder='big')

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

def multicall_fetch(token_addresses: list[str], w3: Web3) -> list[dict]:
    multicall = w3.eth.contract(
        address=Web3.to_checksum_address(MULTICALL3_ADDRESS),
        abi=MULTICALL3_ABI
    )

    calls = []
    for addr in token_addresses:
        target = Web3.to_checksum_address(addr)
        calls.append((target, True, SIG_NAME))
        calls.append((target, True, SIG_SYMBOL))
        calls.append((target, True, SIG_DECIMALS))
        calls.append((target, True, SIG_TOTAL_SUPPLY))

    raw = multicall.functions.aggregate3(calls).call()

    metadata = []
    for i, addr in enumerate(token_addresses):
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


def fetch_token_metadata(rpc: str, start_block: int = None, end_block: int = None, tokens: list = None, out: str = "data/reference_data/token_metadata.csv"):

    w3 = Web3(Web3.HTTPProvider(rpc))
    if not w3.is_connected():
        print("Failed to connect to RPC")
        exit(1)

    tokens_to_fetch = set()
    if tokens:
        tokens_to_fetch.update([t.lower() for t in tokens])
        
    if start_block is not None and end_block is not None:
        import pandas as pd
        path = f"data/stage1_data/token_transfers/block_start={start_block}/block_end={end_block}/data.parquet"
        if os.path.exists(path):
            df = pd.read_parquet(path, columns=["token_address"])
            stage1_tokens = df["token_address"].dropna().unique().tolist()
            tokens_to_fetch.update([t.lower() for t in stage1_tokens])
            print(f"Found {len(stage1_tokens)} unique tokens from stage1 transfers.")
        else:
            print(f"Warning: Stage 1 data not found at {path}")
            
    if not tokens_to_fetch:
        print("No tokens to fetch. Please provide --tokens or a valid block range (--start-block and --end-block).")
        exit(1)

    existing_data = []
    existing_tokens = set()
    if os.path.exists(out):
        with open(out, 'r', newline='') as f:
            reader = csv.DictReader(f)
            for row in reader:
                existing_data.append(row)
                if 'token_address' in row and row['token_address']:
                    existing_tokens.add(row['token_address'].lower())
                    
    missing_tokens = [t for t in tokens_to_fetch if t not in existing_tokens]
    
    if not missing_tokens:
        print(f"All {len(tokens_to_fetch)} tokens already have metadata in {out}. Nothing new to fetch.")
        return
        
    print(f"Found {len(missing_tokens)} missing tokens. Fetching metadata via multicall...")
    new_data = multicall_fetch(missing_tokens, w3)

    all_data = existing_data + new_data

    os.makedirs(os.path.dirname(out), exist_ok=True)
    with open(out, 'w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=["token_address", "token_name", "symbol", "decimals", "total_supply", "supply_source"])
        writer.writeheader()
        writer.writerows(all_data)
    
    print(f"Successfully saved {len(new_data)} new token records to {out} (Total records: {len(all_data)})")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Fetch ERC20 token metadata via Multicall3")
    parser.add_argument("--rpc", required=True, help="Ethereum RPC URL")
    parser.add_argument("--tokens", nargs="*", help="List of token addresses")
    parser.add_argument("--start-block", type=int, help="Extract tokens from stage1 data starting at this block")
    parser.add_argument("--end-block", type=int, help="Extract tokens from stage1 data ending at this block")
    parser.add_argument("--out", default="data/reference_data/token_metadata.csv", help="Output CSV path")
    args = parser.parse_args()

    fetch_token_metadata(rpc=args.rpc, start_block=args.start_block, end_block=args.end_block, tokens=args.tokens, out=args.out)