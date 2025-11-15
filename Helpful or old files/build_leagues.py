# build_leagues.py v1.2
#
# WHAT'S NEW (v1.2):
# - SCHEMA FIX: Removed non-existent columns from the 'competitions'
#   INSERT query (e.g., number_of_available_seasons). The query
#   now matches our simple schema and will stop the crash.
#
# WHAT'S NEW (v1.1):
# - SCHEMA FIX: Removed 'parent_area_id' from the INSERT query in
#   get_or_create_area.
#
# WHAT IT DOES:
# 1. Connects to API-Sports.
# 2. Fetches all leagues for the 2024 season.
# 3. Inserts *only* the league information into your 'competitions' table.

import os
import time
import logging
import requests
import psycopg2
import json
from psycopg2.pool import ThreadedConnectionPool
from psycopg2.extras import execute_values, RealDictCursor
from dotenv import load_dotenv
from datetime import datetime, UTC

# ============ CONFIG & LOGGING ============
load_dotenv()
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(threadName)s] [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

# --- Config ---
# We use 2024 because the API log showed this is the
# historical season your plan allows.
AS_SEASON_TO_FETCH = 2024
MAX_DB_CONNECTIONS = 10
AS_ID_OFFSET = 1_000_000

# ============ CONNECT ============
try:
    db_url = os.getenv("DATABASE_URL")
    if not db_url:
        logging.error(
            "DATABASE_URL not found. Check .env file or Streamlit Secrets."
        )
        exit(1)

    if db_url.startswith("postgresql+psycopg://"):
        logging.warning(
            "DSN prefix 'postgresql+psycopg://' found, "
            "correcting to 'postgresql://'."
        )
        db_url = db_url.replace("postgresql+psycopg://", "postgresql://", 1)

    db_pool = ThreadedConnectionPool(
        minconn=1,
        maxconn=MAX_DB_CONNECTIONS,
        dsn=db_url,
        cursor_factory=RealDictCursor
    )
    logging.info(f"Database connection pool created (Max: {MAX_DB_CONNECTIONS}).")
except Exception as e:
    logging.error(f"DB connection pool failed: {e}")
    exit(1)

# ============ AS API (API-SPORTS) CALLER ============
AS_API_KEY = os.getenv("API_SPORTS_KEY")
AS_BASE_URL = "https://v3.football.api-sports.io"
AS_HEADERS = {"x-apisports-key": AS_API_KEY}
http_session = requests.Session()

def api_call_as(endpoint: str, params: dict = None) -> list:
    """
    Makes a single API call to api-sports.io (AS).
    """
    if not AS_API_KEY:
        raise Exception("API_SPORTS_KEY is not configured.")

    try:
        logging.info(f"Calling AS endpoint: {endpoint} with params: {params}")
        r = http_session.get(
            f"{AS_BASE_URL}{endpoint}",
            headers=AS_HEADERS,
            params=params,
            timeout=30,
        )
        r.raise_for_status()
        data = r.json()

        if not data.get("response") and data.get("errors"):
            logging.error(f"AS API Error: {data['errors']}")
            return []
        
        logging.info(f"AS API call successful, {data.get('results', 0)} results.")
        return data.get("response", [])

    except Exception as e:
        logging.error(f"AS API call to {endpoint} failed: {e}")
        raise e

# ============ DATABASE FUNCTIONS ============

def get_or_create_area(cur, area_name: str, country_code: str = None) -> int:
    """
    Finds an existing area or creates a new one.
    This is necessary because AS leagues are tied to countries (areas).
    """
    # Try to find area by name
    cur.execute("SELECT area_id FROM areas WHERE name = %s", (area_name,))
    row = cur.fetchone()
    if row:
        return row['area_id']
    
    # Not found, create it. Find next available ID (simple approach)
    cur.execute("SELECT COALESCE(MAX(area_id), 2272) + 1 AS next_id FROM areas")
    next_id = cur.fetchone()['next_id']
    
    try:
        # This is the v1.1 fix
        cur.execute(
            """
            INSERT INTO areas (area_id, name, code, flag)
            VALUES (%s, %s, %s, NULL)
            """,
            (next_id, area_name, country_code)
        )
        
        logging.info(f"Created new area for {area_name} with ID {next_id}")
        return next_id
    except psycopg2.Error as e:
        logging.error(f"Failed to create new area {area_name}: {e}")
        # Rollback this single insertion if it fails
        cur.connection.rollback()
        # Fallback to a default 'World' area if creation fails
        cur.execute("SELECT area_id FROM areas WHERE name = 'World'")
        world_row = cur.fetchone()
        if world_row:
            return world_row['area_id']
        return 2077 # Default 'World' ID

def upsert_competitions_from_as(cur, as_leagues: list, season_year: int):
    """
    Transforms and upserts AS league data into the competitions table.
    """
    if not as_leagues:
        logging.info("No AS leagues to upsert.")
        return 0

    values = []
    # Get one timestamp for the entire batch
    now = datetime.now(UTC).isoformat()
    
    for item in as_leagues:
        league = item.get('league', {})
        country = item.get('country', {})
        
        if not league.get('id'):
            continue
            
        as_league_id = league['id']
        
        # Get or create the Area (Country)
        area_id = get_or_create_area(cur, country.get('name'), country.get('code'))
        
        # Add offset to AS ID to prevent collision with FD IDs
        offset_id = as_league_id + AS_ID_OFFSET

        # --- START OF FIX (v1.2) ---
        # This tuple now matches our schema exactly
        values.append((
            offset_id,
            area_id,
            league.get('name'),
            None,  # code
            league.get('type'),
            league.get('logo'),
            now,   # last_updated
            as_league_id # as_competition_id
        ))
        # --- END OF FIX ---

    if not values:
        return 0

    # --- START OF FIX (v1.2) ---
    # This SQL query also matches our schema exactly
    sql = """
    INSERT INTO competitions (
        competition_id, area_id, name, code, type, emblem,
        last_updated, as_competition_id
    )
    VALUES %s
    ON CONFLICT (as_competition_id) DO UPDATE SET
        name = EXCLUDED.name,
        type = EXCLUDED.type,
        emblem = EXCLUDED.emblem,
        area_id = EXCLUDED.area_id,
        last_updated = EXCLUDED.last_updated;
    """
    # --- END OF FIX ---
    
    execute_values(cur, sql, values, page_size=200)
    logging.info(f"Successfully upserted {len(values)} AS competitions.")
    return len(values)

# ============ MAIN ============
def main():
    conn = None
    try:
        # 1. Fetch AS Leagues
        as_leagues = api_call_as(
            "/leagues",
            {"season": AS_SEASON_TO_FETCH}
        )
        
        if not as_leagues:
            logging.error(
                f"No leagues found from AS for season {AS_SEASON_TO_FETCH}. "
                "Check plan or API key."
            )
            return

        # 2. Upsert to DB
        conn = db_pool.getconn()
        with conn.cursor() as cur:
            upsert_competitions_from_as(cur, as_leagues, AS_SEASON_TO_FETCH)
            conn.commit()
            
        logging.info("--- LEAGUE BUILDER SCRIPT FINISHED ---")
        logging.info(
            "All known AS leagues are now in your database. "
            "You can now run sync.py."
        )

    except Exception as e:
        logging.error(f"Main process failed: {e}")
        if conn:
            conn.rollback()
    finally:
        if conn:
            db_pool.putconn(conn)
        if 'db_pool' in globals():
            db_pool.closeall()
            logging.info("Database connection pool closed.")

if __name__ == "__main__":
    main()