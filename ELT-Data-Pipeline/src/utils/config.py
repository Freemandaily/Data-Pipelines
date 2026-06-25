import os
import psycopg2
from psycopg2 import sql
from psycopg2.extras import execute_values
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

def get_db_connection():
    """Establish and return a connection to the PostgreSQL database."""
    schema = os.getenv("DB_SCHEMA", "public")
    conn = psycopg2.connect(
        host=os.getenv("DB_HOST", "localhost"),
        port=os.getenv("DB_PORT", "5432"),
        dbname=os.getenv("DB_NAME", "postgres"),
        user=os.getenv("DB_USER", "postgres"),
        password=os.getenv("DB_PASSWORD", "your_password"),
        options=f'-c search_path="{schema}"'
    )
    return conn

def init_db():
    """Initialize database tables if they do not exist."""
    schema = os.getenv("DB_SCHEMA", "public")
    
    # Ensure the schema exists before connecting with it as search_path
    conn = psycopg2.connect(
        host=os.getenv("DB_HOST", "localhost"),
        port=os.getenv("DB_PORT", "5432"),
        dbname=os.getenv("DB_NAME", "postgres"),
        user=os.getenv("DB_USER", "postgres"),
        password=os.getenv("DB_PASSWORD", "your_password")
    )
    cursor = conn.cursor()
    cursor.execute(sql.SQL("CREATE SCHEMA IF NOT EXISTS {}").format(sql.Identifier(schema)))
    conn.commit()
    cursor.close()
    conn.close()

    conn = get_db_connection()
    cursor = conn.cursor()
    
    # Create watermark table
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS watermark (
            pipeline_name TEXT PRIMARY KEY,
            last_block INTEGER,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    ''')
    
    # Create blocks table
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS blocks (
            number BIGINT PRIMARY KEY,
            timestamp TIMESTAMP,
            hash TEXT,
            gas_used BIGINT,
            gas_limit BIGINT
        )
    ''')
    
    # Create transactions table
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS transactions (
            hash TEXT PRIMARY KEY,
            block_number BIGINT,
            transaction_index INTEGER,
            from_address TEXT,
            to_address TEXT,
            value NUMERIC,
            gas NUMERIC,
            gas_price NUMERIC
        )
    ''')
    
    # Create traces table
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS traces (
            id SERIAL PRIMARY KEY,
            transaction_hash TEXT,
            block_number BIGINT,
            trace_address TEXT,
            call_type TEXT,
            error TEXT,
            from_address TEXT,
            to_address TEXT,
            value NUMERIC,
            UNIQUE(transaction_hash, trace_address)
        )
    ''')
    
    # Create logs table
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS logs (
            id SERIAL PRIMARY KEY,
            transaction_hash TEXT,
            block_number BIGINT,
            log_index INTEGER,
            address TEXT,
            data TEXT,
            topics TEXT,
            UNIQUE(transaction_hash, log_index)
        )
    ''')
    
    # Create token_metadata table
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS token_metadata (
            token_address TEXT PRIMARY KEY,
            token_name TEXT,
            symbol TEXT,
            decimals INTEGER,
            total_supply NUMERIC,
            supply_source TEXT
        )
    ''')
    
    
    conn.commit()
    cursor.close()
    conn.close()

if __name__ == "__main__":
    init_db()
    print("Database initialized successfully.")
