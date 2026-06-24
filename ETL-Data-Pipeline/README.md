# Blockchain Data ETL Pipeline

A production-grade ETL pipeline designed to extract, transform, and load Ethereum blockchain data into a local data warehouse for analytical querying.

## Architecture Overview

This pipeline uses a modern data stack to process raw blockchain data into a structured dimensional model:

1. **Extraction (Subsquid & RPC):** 
   - Uses Subsquid's (SQD) streaming endpoint to rapidly fetch historical blocks, transactions, traces, and logs.
   - Uses an Ethereum RPC node to fetch on-chain Token metadata (symbols, decimals).
   - Extracts data in chunks and saves it locally in Parquet format.

2. **Transformation (Apache Spark):** 
   - **Stage 1 (Clean):** Normalizes hex strings to numerical types (e.g., `DecimalType`), lowercases addresses, and isolates ERC-20 token transfers from raw logs.
   - **Stage 2 (Enrich):** Joins token transfers with on-chain metadata to calculate human-readable token amounts.
   - **Stage 3 (Transfer Extraction):** Unifies native ETH and ERC-20 transfers into a standardized schema.
   - **Stage 4 (Materialize):** Prepares the final `fact_transfers` and `dim_transactions` tables and executes strict data quality validations.

3. **Storage (DuckDB):** 
   - Clean, validated Parquet files are loaded into an embedded DuckDB data warehouse (`blockchain.duckdb`). 
   - The loader implements idempotent upserts: it deletes existing data for the current block range before inserting, ensuring no duplicates.

4. **Orchestration (Apache Airflow):** 
   - The pipeline is orchestrated using Airflow's TaskFlow API. 
   - It runs sequentially every 15 minutes. 
   - A local SQLite database (`watermark.db`) tracks pipeline state dynamically to calculate the next block ranges.

## Setup and Running

### Prerequisites
- Python 3.10+
- Docker & Docker Compose (for Airflow orchestration)
- Java (for Apache Spark execution)

### Environment Variables
Create an `.env` file in the `airflow/` directory containing your RPC URL and Airflow user ID:
```env
ETH_RPC_URL="<YOUR_ETHEREUM_RPC_URL>"
AIRFLOW_UID=1000
```

### Running via Airflow
The easiest way to run the orchestrated pipeline is using the provided Docker Compose setup:

```bash
cd airflow
docker-compose up -d
```

Access the Airflow UI at `http://localhost:8080`, enable the `blockchain_etl_pipeline` DAG, and monitor the sequential task execution.

## Project Structure

```text
├── airflow/
│   ├── dags/                  # Airflow DAG definition (blockchain_etl_dag.py)
│   ├── docker-compose.yml     # Airflow container orchestration
├── src/
│   ├── extractors/            # Scripts for pulling data from Subsquid and RPCs
│   ├── transformers/          # PySpark jobs (stage1 to stage4)
├── data/
│   ├── raw_data/              # Initial Parquet extracts
│   ├── stage*_data/           # Intermediate processed Parquet files
│   ├── warehouse/             # Contains the final DuckDB database
```
