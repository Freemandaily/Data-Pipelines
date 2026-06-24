import os
import sys
from datetime import datetime, timedelta

from airflow.sdk import dag, task
from dotenv import load_dotenv

load_dotenv()

current_dir = os.path.dirname(os.path.abspath(__file__))
if os.path.exists(os.path.join(current_dir, '..', 'src')):
    # Docker environment: src is mounted at /opt/airflow/src
    PROJECT_ROOT = os.path.abspath(os.path.join(current_dir, '..'))
else:
    # Local environment: dags is in Data-pipeline-ETL/airflow/dags
    PROJECT_ROOT = os.path.abspath(os.path.join(current_dir, '../../'))


if PROJECT_ROOT not in sys.path:
    sys.path.append(PROJECT_ROOT)

DEFAULT_ARGS = {
    "owner": "freeman",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

@dag(
    dag_id="blockchain_etl_pipeline",
    default_args=DEFAULT_ARGS,
    description="Blockchain ETL pipeline using TaskFlow API",
    schedule="*/15 * * * *",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["blockchain", "etl"],
)
def blockchain_etl_pipeline():
    
    @task(multiple_outputs=True)
    def determine_block_range():
        import sqlite3
        db_path = os.path.join(PROJECT_ROOT, "watermark.db")
        start = 19_000_000
        try:
            if os.path.exists(db_path):
                conn = sqlite3.connect(db_path)
                conn.execute('''
                    CREATE TABLE IF NOT EXISTS watermark (
                        pipeline_name TEXT PRIMARY KEY,
                        last_block INTEGER,
                        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )
                ''')
                row = conn.execute("SELECT last_block FROM watermark WHERE pipeline_name = 'sqd_main'").fetchone()
                if row:
                    start = row[0] + 1
                conn.close()
        except Exception as e:
            print(f"Could not read watermark, using default: {e}")
            
        end = start + 999 # Process 1000 blocks per DAG run (extract.py will chunk this into batches of 10 internally)
        print(f"Determined block range: {start} to {end}")
        return {"start": start, "end": end}

    @task
    def extract(start: int, end: int):
        from src.extractors.extract import run_extraction
        os.chdir(PROJECT_ROOT)
        run_extraction(start_block=start, end_block=end)

    @task
    def stage1_clean(start: int, end: int):
        from src.transformers.stage1_clean import run_stage1
        os.chdir(PROJECT_ROOT)
        run_stage1(start_block=start, end_block=end)

    @task
    def fetch_metadata(start: int, end: int):
        from src.extractors.fetch_token_metadata import fetch_token_metadata
        import os
        os.chdir(PROJECT_ROOT)
        # Try to get RPC URL from environment variable
        rpc = os.environ.get("ETH_RPC_URL")
        if rpc:
            fetch_token_metadata(rpc=rpc, start_block=start, end_block=end)
        else:
            print("Skipping Token Metadata fetch (No ETH_RPC_URL environment variable provided).")

    @task
    def stage2_enrich(start: int, end: int):
        from src.transformers.stage2_enrich import run_stage2
        os.chdir(PROJECT_ROOT)
        run_stage2(start_block=start, end_block=end)

    @task
    def stage3_transfer_extraction(start: int, end: int):
        from src.transformers.stage3_transfer_extraction import run_stage3
        os.chdir(PROJECT_ROOT)
        run_stage3(start_block=start, end_block=end)

    @task
    def stage4_materialize(start: int, end: int):
        from src.transformers.stage4_materialize import run_stage4
        os.chdir(PROJECT_ROOT)
        run_stage4(start_block=start, end_block=end)

    # DAG orchestration
    block_range = determine_block_range()
    
    start_block = block_range["start"]
    end_block = block_range["end"]

    # Define the sequential dependencies
    extract_task = extract(start_block, end_block)
    stage1_task = stage1_clean(start_block, end_block)
    fetch_metadata_task = fetch_metadata(start_block, end_block)
    stage2_task = stage2_enrich(start_block, end_block)
    stage3_task = stage3_transfer_extraction(start_block, end_block)
    stage4_task = stage4_materialize(start_block, end_block)

    extract_task >> stage1_task >> fetch_metadata_task >> stage2_task >> stage3_task >> stage4_task

# Instantiate the DAG
pipeline_dag = blockchain_etl_pipeline()
