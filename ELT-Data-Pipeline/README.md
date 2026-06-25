# Blockchain Data ELT Pipeline

A production-grade ELT (Extract, Load, Transform) pipeline designed to extract Ethereum blockchain data, load it directly into PostgreSQL, and transform it in-database using dbt (Data Build Tool) for analytical querying.

## Architecture Overview

This pipeline uses a modern data stack to process raw blockchain data into a structured dimensional model entirely within the database:

1. **Extraction & Load (Subsquid, RPC & Python):** 
   - Uses Subsquid's (SQD) streaming endpoint to rapidly fetch historical blocks, transactions, traces, and logs.
   - Loads the extracted raw JSON data directly into the PostgreSQL landing tables.
   - Uses an Ethereum RPC node via `web3.py` to dynamically fetch missing on-chain Token metadata (symbols, decimals) based on the ingested logs.

2. **Transformation (dbt - Data Build Tool):** 
   - **Staging (`staging`):** Cleans and normalizes raw data types (e.g., parsing hex strings into numeric values, lowercasing addresses) and prepares base tables for blocks, logs, and token metadata.
   - **Intermediate (`intermediate`):** Joins and filters staging data to isolate meaningful domain entities, such as ERC-20 and native ETH transfers.
   - **Marts (`marts`):** Builds the final dimensional models (e.g., `fct_token_transfers`, `mart_daily_token_activity`, `mart_daily_network_activity`) for downstream BI tools and analytics.

3. **Storage (PostgreSQL):** 
   - A single PostgreSQL database serves as both the landing area for raw data and the data warehouse for transformed dimensional models.

4. **Orchestration (Apache Airflow):** 
   - The pipeline is orchestrated using Airflow's `BashOperator` to sequentially trigger extraction, token metadata fetching, and dbt models execution.
   - It runs on a scheduled basis (e.g., every 15 minutes). 
   - A database watermark table tracks pipeline state dynamically to calculate the next block ranges for incremental processing.

## Setup and Running

### Prerequisites
- Python 3.10+
- Docker & Docker Compose (for Airflow orchestration and PostgreSQL)

### Environment Variables
Create an `.env` file in the `airflow/` directory containing your RPC URL and Airflow user ID:
```env
DB_HOST=host.docker.internal
DB_PORT=Port
DB_NAME=db-name
DB_USER=user
DB_PASSWORD=password
RPC_URL="<YOUR_ETHEREUM_RPC_URL>"
AIRFLOW_UID=1000
DB_SCHEMA=ELT_DATA
```

### Running via Airflow
The easiest way to run the orchestrated pipeline is using the provided Docker Compose setup:

```bash
cd airflow
docker compose up --build -d
```

Access the Airflow UI at `http://localhost:8080`, enable the `ethereum_elt_pipeline` DAG, and monitor the sequential task execution.

## Project Structure

```text
├── airflow/
│   ├── dags/                  # Airflow DAG definition (pipeline_flow.py)
│   ├── dockerfile             # Custom Airflow image with dbt-postgres dependencies
│   ├── docker-compose.yml     # Airflow container orchestration
├── dbt_model/
│   ├── models/                # dbt SQL models (staging, intermediate, marts)
│   ├── dbt_project.yml        # dbt project configuration
│   ├── profiles.yml           # dbt connection profiles mapped to env vars
├── src/
│   ├── extract.py             # Pulls data from Subsquid and loads to Postgres
│   ├── fetch_token_metadata.py # Fetches token symbols/decimals from RPC
│   ├── utils/                 # DB connection and config utilities
```
