# db.py v1.0
# This file provides the psycopg2 ThreadedConnectionPool
# for the Streamlit application.

import os
import logging
import psycopg2
from psycopg2.pool import ThreadedConnectionPool
from psycopg2.extras import RealDictCursor
from dotenv import load_dotenv

load_dotenv()

# --- Config ---
# Use a slightly larger pool for the web app, as it may serve
# multiple users concurrently.
MIN_CONN = 2
MAX_CONN = 10 

db_url = os.getenv("DATABASE_URL")
if not db_url:
    logging.error("FATAL: DATABASE_URL not set.")
    # This will fail loudly in Streamlit, which is good.
    raise ValueError("DATABASE_URL environment variable not set.")

if db_url.startswith("postgresql+psycopg://"):
    db_url = db_url.replace("postgresql+psycopg://", "postgresql://", 1)

# --- Global Connection Pool ---
# We initialize this once when the module is imported.
try:
    db_pool = ThreadedConnectionPool(
        minconn=MIN_CONN,
        maxconn=MAX_CONN,
        dsn=db_url,
        cursor_factory=RealDictCursor  # Use RealDictCursor globally
    )
    logging.info(f"Streamlit DB pool created (Min: {MIN_CONN}, Max: {MAX_CONN}).")
except (psycopg2.OperationalError, Exception) as e:
    logging.error(f"FATAL: Failed to create DB pool: {e}")
    # This will prevent the app from starting
    db_pool = None 

def test_connection():
    """Tests the connection pool by getting and putting a connection."""
    if not db_pool:
        logging.error("DB Pool is not initialized.")
        return False
        
    conn = None
    try:
        conn = db_pool.getconn()
        with conn.cursor() as cur:
            cur.execute("SELECT 1")
        logging.info("Database connection test successful.")
        return True
    except Exception as e:
        logging.error(f"Database connection test failed: {e}")
        return False
    finally:
        if conn:
            db_pool.putconn(conn)

# Note: We do not provide a 'Session' as we are not using SQLAlchemy ORM.
# Other files will import 'db_pool' directly.