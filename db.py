# db.py v1.18
#
# WHAT'S NEW (v1.18 - Code Cleanup):
# - FIX: Corrected the `store_predictions_db` function (which was unused)
#   to use `fixture_id` instead of the old `match_id`.
#   This makes it consistent with the database schema and predictor.py.

import os
import json
import logging
import psycopg2
import pytz
from datetime import datetime, timezone, timedelta
from psycopg2.pool import ThreadedConnectionPool
from psycopg2.extras import execute_values, RealDictCursor
from dotenv import load_dotenv
from typing import Optional, Any, List, Dict

load_dotenv()

# ============ CONFIGURATION ============

# Database connection details from environment variables
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_NAME = os.getenv("DB_NAME", "football_db")
DB_USER = os.getenv("DB_USER", "postgres")
DB_PASS = os.getenv("DB_PASS", "postgres")
DB_PORT = os.getenv("DB_PORT", "5432")

# Connection pool setup
db_pool = None
MAX_CONNECTIONS = 10
MIN_CONNECTIONS = 2

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')


def initialize_pool():
    """Initializes the global connection pool."""
    global db_pool
    if db_pool is None:
        try:
            db_pool = ThreadedConnectionPool(
                minconn=MIN_CONNECTIONS,
                maxconn=MAX_CONNECTIONS,
                host=DB_HOST,
                database=DB_NAME,
                user=DB_USER,
                password=DB_PASS,
                port=DB_PORT,
                # Supabase requires SSL
                sslmode='require'
            )
            logging.info("Database connection pool initialized successfully.")
        except Exception as e:
            logging.error(f"Error initializing DB pool: {e}")
            raise

# Ensure the pool is initialized on import
try:
    initialize_pool()
except Exception:
    logging.warning("Failed to initialize database pool on script start.")
    pass

# ============ DB UTILITY FUNCTIONS (For Streamlit App) ============

def get_last_updated_time() -> Optional[datetime]:
    """
    v1.15: Fetches the timestamp of the most recently *completed* match
    as a proxy for when the database was last updated with results.
    """
    conn = None
    try:
        conn = db_pool.getconn()
        with conn.cursor() as cur:
            # v1.15 FIX: Query for max date from finished matches
            sql = "SELECT MAX(date) as last_update FROM fixtures WHERE status_short = 'FT'"
            cur.execute(sql)
            result = cur.fetchone()
            if result and result[0]:
                # Ensure the result is timezone-aware (assuming 'date' is UTC)
                if result[0].tzinfo is None:
                    return result[0].replace(tzinfo=pytz.utc)
                return result[0]
            return None
    except Exception as e:
        logging.error(f"Failed to get last updated time: {e}")
        return None
    finally:
        if conn:
            db_pool.putconn(conn)

def get_match_counts() -> Dict[str, int]:
    """
    Fetches the count of matches grouped by status.
    """
    conn = None
    counts = {}
    try:
        conn = db_pool.getconn()
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            # v1.14 FIX: Querying the correct column 'status_short'
            sql = """
            SELECT status_short, COUNT(*)
            FROM fixtures
            GROUP BY status_short
            """
            cur.execute(sql)
            rows = cur.fetchall()
            # Map statuses to app.py's expected keys
            for row in rows:
                status = row['status_short']
                count = row['count']
                if status in ('NS', 'TBD', 'PST'):
                    counts['UPCOMING'] = counts.get('UPCOMING', 0) + count
                elif status in ('FT', 'AET', 'PEN'):
                    counts['PAST'] = counts.get('PAST', 0) + count
                else:
                    # e.g., 'LIVE', 'HT', '1H', '2H', 'INT'
                    counts['OTHER'] = counts.get('OTHER', 0) + count
    except Exception as e:
        logging.error(f"Error fetching match counts: {e}")
    finally:
        if conn:
            db_pool.putconn(conn)
    return counts

def count_standings_lists() -> int:
    """
    Counts the total number of standing entries in the 'standings' table.
    """
    conn = None
    try:
        conn = db_pool.getconn()
        with conn.cursor() as cur:
            sql = "SELECT COUNT(*) FROM standings"
            cur.execute(sql)
            return cur.fetchone()[0]
    except Exception as e:
        logging.error(f"Failed to count table standings: {e}")
        return 0
    finally:
        if conn:
            db_pool.putconn(conn)

def get_all_leagues(conn=None) -> List[Dict[str, Any]]:
    """Fetches all unique league codes and names."""
    should_close_conn = False
    if conn is None:
        conn = db_pool.getconn()
        should_close_conn = True

    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            sql = "SELECT league_id, name FROM leagues ORDER BY name"
            cur.execute(sql)
            return cur.fetchall()
    except Exception as e:
        logging.error(f"Error fetching leagues: {e}")
        return []
    finally:
        if should_close_conn and conn:
            db_pool.putconn(conn)

def search_teams_and_competitions(search_key: str) -> List[Dict[str, Any]]:
    """
    v1.14: Added function (was missing).
    Searches for teams and competitions matching the search key.
    """
    conn = None
    try:
        conn = db_pool.getconn()
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            sql = """
            (
                SELECT
                    l.league_id as id,
                    l.name,
                    l.logo_url as emblem,
                    'competition' as type
                FROM leagues l
                WHERE l.name ILIKE %s
            )
            UNION ALL
            (
                SELECT
                    t.team_id as id,
                    t.name,
                    t.logo_url as emblem,
                    'team' as type
                FROM teams t
                WHERE t.name ILIKE %s
            )
            LIMIT 10;
            """
            search_term = f"%{search_key}%"
            cur.execute(sql, (search_term, search_term))
            return cur.fetchall()
    except Exception as e:
        logging.error(f"Error during search: {e}")
        return []
    finally:
        if conn:
            db_pool.putconn(conn)


def get_filtered_matches(
    date_from: str,
    date_to: str,
    predictions_only: bool = False,
    limit: Optional[int] = None,
    offset: int = 0,
    search_query: Optional[str] = None,
    competition_code: Optional[str] = None
) -> List[Dict[str, Any]]:
    """
    v1.17: Corrected 'hl.code' to 'hl.league_id' to match schema.
    Fetches fixture data with dynamic filters for date, predictions, search,
    competition, and pagination.
    """
    conn = None
    params = []
    
    # Base query with corrected column names and joins
    sql = """
        SELECT
            f.fixture_id,
            f.date as utc_date, -- Alias for compatibility with app
            f.status_short as status, -- Alias for compatibility with app
            f.goals_home as home_score, -- Alias
            f.goals_away as away_score, -- Alias
            p.prediction_data,
            
            -- v1.17 FIX: Use league_id as the competition_code
            hl.league_id as competition_code,
            
            ht.name as home_team_name,
            ht.logo_url as home_team_crest, -- Use logo_url
            at.name as away_team_name,
            at.logo_url as away_team_crest, -- Use logo_url
            
            -- v1.16: Add competition data directly
            hl.name as competition_name,
            hl.logo_url as competition_crest,
            hl.country_name as competition_country
            
        FROM fixtures f
        JOIN leagues hl ON f.league_id = hl.league_id
        JOIN teams ht ON f.home_team_id = ht.team_id
        JOIN teams at ON f.away_team_id = at.team_id
        LEFT JOIN predictions p ON f.fixture_id = p.fixture_id
    """

    where_clauses = []

    # 1. Date Filter (Mandatory)
    try:
        date_start_utc = datetime.fromisoformat(date_from).astimezone(timezone.utc)
        date_end_utc = datetime.fromisoformat(date_to).astimezone(timezone.utc)
    except ValueError:
        date_start_utc = datetime.strptime(date_from, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
        date_end_utc = datetime.strptime(date_to, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)

    where_clauses.append("f.date BETWEEN %s AND %s")
    params.extend([date_start_utc, date_end_utc])

    # 2. Predictions Only Filter
    if predictions_only:
        where_clauses.append("p.prediction_data IS NOT NULL AND p.prediction_data->>'h2h' != '[]'")

    # 3. Competition Filter
    if competition_code:
        # v1.17 FIX: Filter on hl.league_id, not hl.code
        where_clauses.append("hl.league_id = %s")
        params.append(competition_code)

    # 4. Search Query Filter
    if search_query:
        where_clauses.append("(ht.name ILIKE %s OR at.name ILIKE %s OR hl.name ILIKE %s)")
        search_term = f"%{search_query}%"
        params.extend([search_term, search_term, search_term])

    # Assemble final query
    if where_clauses:
        sql += " WHERE " + " AND ".join(where_clauses)

    sql += " ORDER BY f.date ASC"

    # 5. Pagination
    if limit is not None:
        sql += " LIMIT %s"
        params.append(limit)
    
    if offset > 0:
        sql += " OFFSET %s"
        params.append(offset)

    # --- Execute Query ---
    try:
        conn = db_pool.getconn()
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(sql, tuple(params))
            matches_data = cur.fetchall()
            return matches_data
    except Exception as e:
        logging.error(f"Error fetching filtered matches: {e}")
        logging.error(f"Failing SQL (approx): {sql}")
        logging.error(f"Params: {params}")
        return []
    finally:
        if conn:
            db_pool.putconn(conn)


# ============ DB MANIPULATION FUNCTIONS (For populator/predictor) ============

# v1.18: This function is not used by predictor.py (which uses db_utils.py)
# but is corrected here to match the schema for future-proofing.
def store_predictions_db(conn, predictions_to_store: List[Dict[str, Any]]):
    """
    Stores or updates predictions in the database.
    """
    if not predictions_to_store:
        return

    insert_data = []
    
    # Local JSON encoder for datetime
    class DateTimeEncoder(json.JSONEncoder):
        def default(self, obj):
            if isinstance(obj, datetime):
                return obj.isoformat()
            return json.JSONEncoder.default(self, obj)

    for pred in predictions_to_store:
        # v1.18 FIX: Use 'fixture_id' and 'predictions'
        fixture_id = pred['fixture_id']
        prediction_json = json.dumps(pred['predictions'], cls=DateTimeEncoder)
        generated_at = datetime.now(timezone.utc)
        insert_data.append((fixture_id, prediction_json, generated_at))

    # v1.18 FIX: Use 'fixture_id' in the query
    query = """
    INSERT INTO predictions (fixture_id, prediction_data, generated_at)
    VALUES (%s, %s::jsonb, %s)
    ON CONFLICT (fixture_id) DO UPDATE
    SET
        prediction_data = EXCLUDED.prediction_data,
        generated_at = EXCLUDED.generated_at;
    """
    try:
        with conn.cursor() as cur:
            execute_values(cur, query, insert_data, page_size=100)
        conn.commit()
        logging.info(f"Successfully stored/updated {len(predictions_to_store)} predictions.")
    except psycopg2.Error as e:
        conn.rollback()
        logging.error(f"PostgreSQL error during prediction storage: {e}")
        raise e
    except Exception as e:
        conn.rollback()
        logging.error(f"General error during prediction storage: {e}")
        raise e

# ============ HELPER FUNCTIONS FOR WIDGETS (JSONB Extraction) ============

def get_h2h_data(prediction_data: dict) -> list:
    if prediction_data:
        return prediction_data.get('h2h', [])
    return []

def get_last_7_home_data(prediction_data: dict) -> list:
    if prediction_data:
        return prediction_data.get('home_last7', [])
    return []

def get_last_7_away_data(prediction_data: dict) -> list:
    if prediction_data:
        return prediction_data.get('away_last7', [])
    return []

def get_tags(prediction_data: dict, team_type: str) -> list:
    if prediction_data:
        if team_type == 'home':
            return prediction_data.get('home_tags', ["Let's learn"])
        elif team_type == 'away':
            return prediction_data.get('away_tags', ["Let's learn"])
    return ["Let's learn"]

# =======================================================================
def close_pool():
    global db_pool
    if db_pool:
        db_pool.closeall()
        db_pool = None
import atexit
atexit.register(close_pool)