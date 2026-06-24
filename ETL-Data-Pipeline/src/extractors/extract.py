"""
This script downloads raw Ethereum data (blocks, transactions, traces, and logs) 
from the Subsquid API. It breaks large requests into smaller chunks 
so the API doesn't crash, and saves the downloaded data as Parquet files.
It also uses an SQLite database to remember which blocks were finished, so it can resume if interrupted.
"""

import requests
import json
import sqlite3
import os
import csv
import argparse
from typing import List, Dict, Any

WATERMARK_DB = "watermark.db"
URL = "https://portal.sqd.dev/datasets/ethereum-mainnet/stream"
BATCH_SIZE = 500  # Number of blocks to process per batch
DEFAULT_START_BLOCK = 19_000_000

def get_watermark(pipeline_name: str) -> int:
    conn = sqlite3.connect(WATERMARK_DB)
    conn.execute('''
        CREATE TABLE IF NOT EXISTS watermark (
            pipeline_name TEXT PRIMARY KEY,
            last_block INTEGER,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    ''')
    row = conn.execute(
        "SELECT last_block FROM watermark WHERE pipeline_name = ?", (pipeline_name,)
    ).fetchone()
    conn.close()
    return row[0] if row else DEFAULT_START_BLOCK

def set_watermark(pipeline_name: str, block: int):
    conn = sqlite3.connect(WATERMARK_DB)
    conn.execute('''
        CREATE TABLE IF NOT EXISTS watermark (
            pipeline_name TEXT PRIMARY KEY,
            last_block INTEGER,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    ''')
    conn.execute(
        "INSERT OR REPLACE INTO watermark (pipeline_name, last_block) VALUES (?, ?)",
        (pipeline_name, block),
    )
    conn.commit()
    conn.close()

def fetch_from_sqd(payload: dict) -> list:
    """Helper function to stream from SQD and return raw blocks."""
    headers = {"Content-Type": "application/json"}
    blocks_raw = []
    
    with requests.post(URL, json=payload, headers=headers, stream=True, timeout=30) as response:
        if response.status_code != 200:
            raise Exception(f"Failed to fetch data: {response.status_code} - {response.text}")
        
        for line in response.iter_lines():
            if not line:
                continue
            try:
                batch = json.loads(line)
            except json.JSONDecodeError:
                continue
            
            if isinstance(batch, list):
                blocks_raw.extend(batch)
            else:
                blocks_raw.append(batch)
                
    return blocks_raw

def extract_blocks(start_block: int, end_block: int) -> List[Dict[str, Any]]:
    print(f"Extracting blocks {start_block} to {end_block}...")
    payload = {
        "type": "evm",
        "fromBlock": start_block,
        "toBlock": end_block,
        "includeAllBlocks": True,
        "fields": {
            "block": {"number": True, "timestamp": True, "hash": True, "gasUsed": True, "gasLimit": True}
        }
    }
    
    raw_blocks = fetch_from_sqd(payload)
    blocks_data = []
    
    for block in raw_blocks:
        header = block.get('header', {})
        blocks_data.append({
            "number": header.get('number'),
            "timestamp": header.get('timestamp'),
            "hash": header.get('hash'),
            "gas_used": header.get('gasUsed'),
            "gas_limit": header.get('gasLimit')
        })
        
    return blocks_data

def extract_transactions(start_block: int, end_block: int) -> List[Dict[str, Any]]:
    print(f"Extracting transactions {start_block} to {end_block}...")
    payload = {
        "type": "evm",
        "fromBlock": start_block,
        "toBlock": end_block,
        "includeAllBlocks": True,
        "transactions": [{}],
        "fields": {
            "block": {"number": True},
            "transaction": {"hash": True, "from": True, "to": True, "value": True, "gas": True, "gasPrice": True, "transactionIndex": True}
        }
    }
    
    raw_blocks = fetch_from_sqd(payload)
    tx_data = []
    
    for block in raw_blocks:
        b_num = block.get('header', {}).get('number')
        for tx in block.get('transactions', []):
            tx_data.append({
                "hash": tx.get('hash'),
                "block_number": b_num,
                "transaction_index": tx.get('transactionIndex'),
                "from_address": tx.get('from'),
                "to_address": tx.get('to'),
                "value": str(int(tx.get('value', '0x0'), 16)) if tx.get('value') else "0",
                "gas": str(int(tx.get('gas', '0x0'), 16)) if tx.get('gas') else "0",
                "gas_price": str(int(tx.get('gasPrice', '0x0'), 16)) if tx.get('gasPrice') else "0",
            })
            
    return tx_data

def extract_traces(start_block: int, end_block: int) -> List[Dict[str, Any]]:
    print(f"Extracting traces {start_block} to {end_block}...")
    payload = {
        "type": "evm",
        "fromBlock": start_block,
        "toBlock": end_block,
        "includeAllBlocks": True,
        "transactions": [{}],  
        "traces": [{}],
        "fields": {
            "block": {"number": True},
            "transaction": {"hash": True, "transactionIndex": True},
            "trace": {"transactionIndex": True, "traceAddress": True, "type": True, "error": True, "callFrom": True, "callTo": True, "callValue": True, "callType": True}
        }
    }
    
    raw_blocks = fetch_from_sqd(payload)
    traces_data = []
    
    for block in raw_blocks:
        b_num = block.get('header', {}).get('number')
        tx_index_to_hash = {tx.get('transactionIndex'): tx.get('hash') for tx in block.get('transactions', [])}
        
        for trace in block.get('traces', []):
            tx_idx = trace.get('transactionIndex')
            traces_data.append({
                "transaction_hash": tx_index_to_hash.get(tx_idx),
                "block_number": b_num,
                "trace_address": json.dumps(trace.get('traceAddress', [])),
                "call_type": trace.get('callType'),
                "error": trace.get('error'),
                "from_address": trace.get('callFrom'),
                "to_address": trace.get('callTo'),
                "value": str(int(trace.get('callValue', '0x0'), 16)) if trace.get('callValue') else "0",
            })
            
    return traces_data

def extract_logs(start_block: int, end_block: int) -> List[Dict[str, Any]]:
    print(f"Extracting logs {start_block} to {end_block}...")
    payload = {
        "type": "evm",
        "fromBlock": start_block,
        "toBlock": end_block,
        "includeAllBlocks": True,
        "transactions": [{}],  
        "logs": [{}],
        "fields": {
            "block": {"number": True},
            "transaction": {"hash": True, "transactionIndex": True},
            "log": {"transactionIndex": True, "logIndex": True, "address": True, "data": True, "topics": True}
        }
    }
    
    raw_blocks = fetch_from_sqd(payload)
    logs_data = []
    
    for block in raw_blocks:
        b_num = block.get('header', {}).get('number')
        tx_index_to_hash = {tx.get('transactionIndex'): tx.get('hash') for tx in block.get('transactions', [])}
        
        for log in block.get('logs', []):
            tx_idx = log.get('transactionIndex')
            logs_data.append({
                "transaction_hash": tx_index_to_hash.get(tx_idx),
                "block_number": b_num,
                "log_index": log.get('logIndex'),
                "address": log.get('address'),
                "data": log.get('data'),
                "topics": json.dumps(log.get('topics', [])),
            })
            
    return logs_data

def save_to_parquet(data_list: List[Dict[str, Any]], data_type: str, full_start: int, full_end: int, chunk_start: int, chunk_end: int):
    if not data_list:
        print(f"No data to save for {data_type}.")
        return

    output_dir = f"data/raw_data/{data_type}/block_start={full_start}/block_end={full_end}"
    os.makedirs(output_dir, exist_ok=True)
    
    output_file = os.path.join(output_dir, f"chunk_{chunk_start}_{chunk_end}.parquet")
    print(f"Saving {len(data_list)} {data_type} to {output_file}...")
    
    import pandas as pd
    df = pd.DataFrame(data_list)
    df.to_parquet(output_file, index=False)

def run_extraction(start_block: int = None, end_block: int = None):
    pipeline_name = "sqd_main"
    
    if start_block is None:
        start_block = get_watermark(pipeline_name) + 1
        
    if end_block is None:
        end_block = start_block + BATCH_SIZE - 1

    current_start = start_block
    
    while current_start <= end_block:
        chunk_end = min(current_start + BATCH_SIZE - 1, end_block)
        print(f"\n--- Extracting Chunk: {current_start} to {chunk_end} ---")
        
        blocks = extract_blocks(current_start, chunk_end)
        save_to_parquet(blocks, "blocks", start_block, end_block, current_start, chunk_end)
        
        txs = extract_transactions(current_start, chunk_end)
        save_to_parquet(txs, "transactions", start_block, end_block, current_start, chunk_end)
        
        traces = extract_traces(current_start, chunk_end)
        save_to_parquet(traces, "traces", start_block, end_block, current_start, chunk_end)
        
        logs = extract_logs(current_start, chunk_end)
        save_to_parquet(logs, "logs", start_block, end_block, current_start, chunk_end)
        
        set_watermark(pipeline_name, chunk_end)
        print(f"Watermark updated to block {chunk_end}")
        
        current_start = chunk_end + 1
        
    print(f"\nSuccessfully extracted all blocks from {start_block} to {end_block}.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Extract EVM data from SQD stream")
    parser.add_argument("--start-block", type=int, help="Starting block number to extract")
    parser.add_argument("--end-block", type=int, help="Ending block number to extract")
    args = parser.parse_args()
    
    run_extraction(start_block=args.start_block, end_block=args.end_block)
