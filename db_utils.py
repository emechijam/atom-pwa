# db_utils.py v1.6 - Centralized DB connection and utilities
"""
Centralized module for PostgreSQL connection pool management and common utilities.

WHAT'S NEW (v1.6 - CRITICAL APP FIXES): 
- CRITICAL FIX 1 (TypeError/Naming): Renamed `get_filtered_fixtures` to 
  `get_filtered_matches` and updated arguments to accept timezone-aware 
  `datetime` objects (`date_from`, `date_to`) to match the Streamlit app's call signature.
- CRITICAL FIX 2 (UI Data): Added `t_home.emblem` and `t_away.emblem` to the SQL 
  `SELECT` query to provide team crests, which are essential for `widgets.py`.
- FEATURE: Added necessary JSONB extraction helper functions (H2H, tags) 
  from the old 'db.py' for use by 'widgets.py'.
- SQL FIX: Confirmed all SQL queries now correctly reference `fixtures` and `standings`.
"""

import os
import logging
import psycopg2
import json
from psycopg2.pool import ThreadedConnectionPool
from psycopg2.extras import RealDictCursor
from dotenv import load_dotenv
from typing import Optional, Any, List, Dict, Tuple
from datetime import datetime, timezone

# Load environment variables
load_dotenv()

# ============ UTILITY CONSTANTS ============
MAX_RETRIES = 3
RETRY_SLEEP_SECONDS = 5
TIMEOUT_SECONDS = 15

# DB Pool Config
POOL_MIN = 1
POOL_MAX = 10 

# Enrichment Config
ENRICHMENT_COOLDOWN_HOURS = 24
ENRICHMENT_BATCH_SIZE = 20

# ============ DB CONNECTION POOL ============
DB_POOL = None

def init_connection_pool():
    """
    Initializes the database connection pool.
    """
    global DB_POOL
    if DB_POOL:
        return

    DB_HOST = os.getenv("DB_HOST", "localhost")
    DB_NAME = os.getenv("DB_NAME", "football_db")
    DB_USER = os.getenv("DB_USER", "postgres")
    DB_PASS = os.getenv("DB_PASS", "postgres")
    DB_PORT = os.getenv("DB_PORT", "5432")

    conn_string = (
        f"host={DB_HOST} dbname={DB_NAME} user={DB_USER} "
        f"password={DB_PASS} port={DB_PORT}"
    )

    # Add SSL requirement if using a pooler URL
    if 'pooler' in DB_HOST:
        conn_string += " sslmode=require"

    try:
        DB_POOL = ThreadedConnectionPool(
            minconn=POOL_MIN,
            maxconn=POOL_MAX,
            dsn=conn_string
        )
        logging.info("Database connection pool initialized successfully.")
        # Attempt to create necessary indexes on startup
        init_db_indexes()
    except Exception as e:
        logging.error(f"FATAL: Failed to initialize DB connection pool: {e}")
        DB_POOL = None


def init_db_indexes():
    """
    Creates necessary indexes for performance if they do not already exist.
    """
    conn = None
    try:
        conn = get_connection()
        if not conn:
            return

        cur = conn.cursor()
        
        indexes = [
            ("idx_fixtures_date", "CREATE INDEX IF NOT EXISTS idx_fixtures_date ON public.fixtures (date)"),
            ("idx_fixtures_league_id", "CREATE INDEX IF NOT EXISTS idx_fixtures_league_id ON public.fixtures (league_id)"),
            ("idx_standings_league_season", "CREATE INDEX IF NOT EXISTS idx_standings_league_season ON public.standings (league_id, season_year, rank)"),
            ("idx_predictions_fixture_id", "CREATE UNIQUE INDEX IF NOT EXISTS idx_predictions_fixture_id ON public.predictions (fixture_id)"),
            ("idx_predictions_data_gin", "CREATE INDEX IF NOT EXISTS idx_predictions_data_gin ON public.predictions USING gin (prediction_data)"),
            ("idx_teams_name", "CREATE INDEX IF NOT EXISTS idx_teams_name ON public.teams (name)"),
            ("idx_league_seasons_key", "CREATE UNIQUE INDEX IF NOT EXISTS idx_league_seasons_key ON public.league_seasons (league_id, season_year)"),
        ]
        
        logging.info("Checking and creating essential database indexes...")
        for name, sql in indexes:
            try:
                cur.execute(sql)
            except Exception as e:
                logging.warning(f"Could not create index {name}: {e}")
        
        conn.commit()
        logging.info("Database index check complete.")

    except Exception as e:
        logging.error(f"Error during index creation: {e}")
    finally:
        release_connection(conn)

def get_connection():
    """Retrieves a connection from the pool, initializing the pool if necessary."""
    if not DB_POOL:
        try:
            init_connection_pool()
        except Exception:
            logging.error("Connection pool is not initialized and initialization failed.")
            return None
            
    try:
        return DB_POOL.getconn()
    except psycopg2.Error as e:
        logging.error(f"Error getting connection from pool: {e}")
        return None

def release_connection(conn):
    """Releases a connection back to the pool."""
    if not DB_POOL:
        return
    if conn:
        try:
            DB_POOL.putconn(conn)
        except psycopg2.Error as e:
            logging.error(f"Error releasing connection: {e}")

def close_all_connections():
    """Closes all connections in the pool."""
    global DB_POOL
    if DB_POOL:
        logging.info("Closing all database connections in the pool...")
        DB_POOL.closeall()
        DB_POOL = None
        logging.info("Database connection pool closed.")

def safe_int(value: Optional[Any]) -> Optional[int]:
    """Safely converts a value to an integer, returning None if conversion fails."""
    if value is None:
        return None
    try:
        return int(value)
    except (ValueError, TypeError):
        return None
        
def safe_str(value: Optional[Any]) -> Optional[str]:
    """Safely converts a value to a string, returning None if conversion fails."""
    if value is None:
        return None
    try:
        return str(value)
    except (ValueError, TypeError):
        return None

# ============ SQL Fetch Functions (Corrected for app compatibility) ============

def get_filtered_matches(
    league_ids: List[int], 
    date_from: datetime, # Changed from start_utc: str
    date_to: datetime,   # Changed from end_utc: str
    status: str
) -> List[Dict[str, Any]]:
    """
    Fetches fixture data with team names, crests, and prediction data for a given 
    date range, league IDs, and status.

    NOTE: The Streamlit app expects league ID (int) input for filtering 
    and timezone-aware datetime objects for the date range.
    """
    conn = None
    try:
        conn = get_connection()
        if not conn:
            return []

        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            sql = """
                SELECT
                    f.fixture_id,
                    f.utc_date,
                    f.status,
                    f.home_score,
                    f.away_score,
                    p.prediction_data,
                    l.code AS competition_code,
                    t_home.name AS home_team_name,
                    t_home.emblem AS home_team_crest, -- CRITICAL for UI
                    t_away.name AS away_team_name,
                    t_away.emblem AS away_team_crest, -- CRITICAL for UI
                    l.name AS league_name,
                    l.fd_code
                FROM public.fixtures f
                JOIN public.teams t_home ON f.home_team_id = t_home.team_id
                JOIN public.teams t_away ON f.away_team_id = t_away.team_id
                LEFT JOIN public.predictions p ON f.fixture_id = p.fixture_id
                -- Join on league_seasons to get the league object
                JOIN public.league_seasons ls ON f.league_season_id = ls.id 
                JOIN public.leagues l ON ls.league_id = l.league_id 
                WHERE 
                    l.league_id = ANY(%s) AND 
                    f.utc_date BETWEEN %s AND %s AND 
                    f.status = %s
                ORDER BY f.utc_date ASC;
            """
            # Ensure datetime objects are UTC before passing to SQL
            start_utc_aware = date_from.astimezone(timezone.utc)
            end_utc_aware = date_to.astimezone(timezone.utc)
            
            cur.execute(sql, (league_ids, start_utc_aware, end_utc_aware, status))
            return cur.fetchall()

    except Exception as e:
        logging.error(f"Error fetching filtered matches: {e}")
        return []
    finally:
        release_connection(conn)

def get_match_counts(league_ids: List[int], date_from: datetime, date_to: datetime) -> Dict[str, int]:
    """
    Counts the number of matches by status for filtering.
    """
    conn = None
    try:
        conn = get_connection()
        if not conn:
            return {}
        
        with conn.cursor() as cur:
            sql = """
                SELECT 
                    f.status, 
                    COUNT(*)
                FROM public.fixtures f
                JOIN public.league_seasons ls ON f.league_season_id = ls.id 
                JOIN public.leagues l ON ls.league_id = l.league_id
                WHERE 
                    l.league_id = ANY(%s) AND 
                    f.utc_date BETWEEN %s AND %s
                GROUP BY f.status;
            """
            # Ensure datetime objects are UTC before passing to SQL
            start_utc_aware = date_from.astimezone(timezone.utc)
            end_utc_aware = date_to.astimezone(timezone.utc)
            
            cur.execute(sql, (league_ids, start_utc_aware, end_utc_aware))
            return dict(cur.fetchall())
            
    except Exception as e:
        logging.error(f"Error fetching match counts: {e}")
        return {}
    finally:
        release_connection(conn)

def get_db_stats() -> Tuple[Optional[str], Optional[int]]:
    """
    Fetches the last updated time from the fixtures table and the count
    from the standings table.
    """
    conn = None
    last_update = None
    standings_count = None
    
    try:
        conn = get_connection()
        if not conn:
            return None, None

        with conn.cursor() as cur:
            # Get last updated time from the 'fixtures' table
            try:
                cur.execute("SELECT MAX(last_updated) as last_update FROM fixtures")
                result = cur.fetchone()
                if result and result[0]:
                    # Format the datetime object to a readable string (in UTC)
                    last_update = result[0].astimezone(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')
            except psycopg2.errors.UndefinedTable:
                logging.error("Failed to get last updated time: relation \"fixtures\" does not exist")
            except Exception as e:
                logging.error(f"Failed to get last updated time: {e}")


            # Get count from the 'standings' table
            try:
                cur.execute("SELECT COUNT(*) FROM standings")
                result = cur.fetchone()
                if result and result[0] is not None:
                    standings_count = int(result[0])
            except psycopg2.errors.UndefinedTable:
                logging.error("Failed to count table standings: relation \"standings\" does not exist")
            except Exception as e:
                logging.error(f"Failed to count table standings: {e}")

    except Exception as e:
        logging.error(f"Error in get_db_stats: {e}")
    finally:
        release_connection(conn)
        
    return last_update, standings_count

# ============ HELPER FUNCTIONS FOR WIDGETS (JSONB Extraction) ============
# These functions are necessary for widgets.py to access prediction data

def get_h2h_data(prediction_data: dict) -> list:
    """Extracts the H2H list from the prediction JSONB."""
    if prediction_data:
        return prediction_data.get('h2h', [])
    return []

def get_last_7_home_data(prediction_data: dict) -> list:
    """Extracts the Home Last 7 list from the prediction JSONB."""
    if prediction_data:
        return prediction_data.get('home_last7', [])
    return []

def get_last_7_away_data(prediction_data: dict) -> list:
    """Extracts the Away Last 7 list from the prediction JSONB."""
    if prediction_data:
        return prediction_data.get('away_last7', [])
    return []

def get_tags(prediction_data: dict, team_type: str) -> list:
    """Extracts the tags (home_tags or away_tags) from the prediction JSONB."""
    if prediction_data:
        if team_type == 'home':
            return prediction_data.get('home_tags', ["Let's learn"])
        elif team_type == 'away':
            return prediction_data.get('away_tags', ["Let's learn"])
    return ["Let's learn"]

# Register the cleanup function
import atexit
atexit.register(close_all_connections)