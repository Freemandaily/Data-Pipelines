import argparse
from src.transformers.stage1_clean import run_stage1
from src.transformers.stage2_enrich import run_stage2
from src.transformers.stage3_transfer_extraction import run_stage3
from src.transformers.stage4_materialize import run_stage4
from src.extractors.fetch_token_metadata import fetch_token_metadata
from src.extractors.extract import run_extraction


def run_pipeline(start_block: int, end_block: int, rpc: str = None):
    """
    Executes the end-to-end data pipeline.
    """
    print(f"Starting pipeline for blocks {start_block} to {end_block}")
    
    print("--- Task 0: Extract Raw Data ---")
    # run_extraction(start_block, end_block)
    
    print("--- Task 1: Clean Raw Data ---")
    run_stage1(start_block, end_block)
    
    print("--- Fetch Token Metadata ---")
    if rpc:
        fetch_token_metadata(rpc=rpc, start_block=start_block, end_block=end_block)
    else:
        print("Skipping Token Metadata fetch (No RPC provided).")
    
    print("--- Task 2: Enrich Transfers & Transactions ---")
    run_stage2(start_block, end_block)
    
    print("--- Task 3: Extract & Unify Transfers ---")
    run_stage3(start_block, end_block)
    
    print("--- Task 4: Materialize to DuckDB ---")
    run_stage4(start_block, end_block)
    
    print(f"Pipeline for {start_block}-{end_block} completed successfully!")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run the end-to-end blockchain ETL pipeline")
    parser.add_argument("--start-block", type=int, required=True, help="Start block number")
    parser.add_argument("--end-block", type=int, required=True, help="End block number")
    parser.add_argument("--rpc", type=str, help="Ethereum RPC URL for fetching token metadata")
    
    args = parser.parse_args()
    run_pipeline(args.start_block, args.end_block, args.rpc)
