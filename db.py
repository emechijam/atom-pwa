# db.py v1.1
#
# WHAT'S NEW (v1.1):
# - DYNAMIC SQL: get_filtered_matches is the new core function,
#   replacing all previous fetch functions. It accepts optional
#   date range, competition code, search_query, and predictions_only
#   filters.
# - SEARCH LOGIC: Implemented search_teams_and_competitions to
#   retrieve clickable teams and competitions from the DB for
#   the new search results view.
# - ALL MATCHES: Added get_all_matches for simple statistical counts.

import os
import logging
import psycopg2
from psycopg2.pool import ThreadedConnectionPool
from psycopg2.extras import RealDictCursor
from dotenv import load_dotenv
from typing import List, Dict, Any, Optional

load_dotenv()

# --- Config and Initialization ---
MIN_CONN = 2
MAX_CONN = 10 

db_url = os.getenv("DATABASE_URL")
if not db_url:
    logging.error("FATAL: DATABASE_URL not set.")
    raise ValueError("DATABASE_URL environment variable not set.")

if db_url.startswith("postgresql+psycopg://"):
    db_url = db_url.replace("postgresql+psycopg://", "postgresql://", 1)

# --- Global Connection Pool ---
try:
    db_pool = ThreadedConnectionPool(
        minconn=MIN_CONN,
        maxconn=MAX_CONN,
        dsn=db_url,
        cursor_factory=RealDictCursor 
    )
    logging.info(f"Streamlit DB pool created (Min: {MIN_CONN}, Max: {MAX_CONN}).")
except (psycopg2.OperationalError, Exception) as e:
    logging.error(f"FATAL: Failed to create DB pool: {e}")
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

def get_db_conn():
    if not db_pool:
        raise ConnectionError("Database pool is not initialized.")
    return db_pool.getconn()

def release_db_conn(conn):
    if db_pool:
        db_pool.putconn(conn)

# --- v1.1: NEW SEARCH AND FILTER FUNCTIONS ---

def get_all_matches() -> List[Dict[str, Any]]:
    """
    Retrieves all matches from the DB. Used only for sidebar stats and initialization.
    """
    conn = None
    try:
        conn = get_db_conn()
        with conn.cursor() as cur:
            cur.execute("""
                SELECT
                    m.*,
                    c.code as competition_code,
                    p.prediction_data
                FROM matches m
                JOIN competitions c ON m.competition_id = c.competition_id
                LEFT JOIN predictions p ON m.match_id = p.match_id
                ORDER BY m.utc_date ASC
            """)
            return cur.fetchall()
    except Exception as e:
        logging.error(f"Error in get_all_matches: {e}")
        return []
    finally:
        if conn:
            release_db_conn(conn)


def get_filtered_matches(date_from: Optional[str] = None, 
                         date_to: Optional[str] = None, 
                         competition_code: Optional[str] = None,
                         search_query: str = "",
                         predictions_only: bool = False) -> List[Dict[str, Any]]:
    """
    Fetches matches based on optional date range, competition, search, and prediction filter.
    """
    conn = None
    try:
        conn = get_db_conn()
        with conn.cursor() as cur:
            # Base query including joins for filtering
            sql = """
            SELECT
                m.match_id,
                m.utc_date,
                m.status,
                m.raw_data,
                c.code as competition_code,
                p.prediction_data
            FROM matches m
            JOIN competitions c ON m.competition_id = c.competition_id
            JOIN teams ht ON m.home_team_id = ht.team_id
            JOIN teams at ON m.away_team_id = at.team_id
            LEFT JOIN predictions p ON m.match_id = p.match_id
            """
            
            # Dynamically build WHERE clause
            where_clauses = []
            params = []

            # 1. Date Range Filter
            if date_from and date_to:
                where_clauses.append("m.utc_date BETWEEN %s AND %s")
                params.extend([date_from, date_to])
            
            # 2. Competition Filter
            if competition_code:
                where_clauses.append("c.code = %s")
                params.append(competition_code)

            # 3. Prediction Filter
            if predictions_only:
                # Checks if prediction_data exists and if 'h2h' is not an empty JSON array
                where_clauses.append(
                    "(p.prediction_data IS NOT NULL AND p.prediction_data -> 'h2h' != '[]'::jsonb)"
                )
            
            # 4. Search Query Filter (Teams and Competition Name)
            if search_query:
                search_term = f"%{search_query}%"
                where_clauses.append(
                    """
                    (ht.name ILIKE %s OR at.name ILIKE %s OR c.name ILIKE %s)
                    """
                )
                params.extend([search_term, search_term, search_term])
                
                # NOTE: Player search is excluded as there is no 'players' table.

            # Assemble the final query
            if where_clauses:
                sql += " WHERE " + " AND ".join(where_clauses)
            
            sql += " ORDER BY m.utc_date ASC;"

            cur.execute(sql, tuple(params))
            return cur.fetchall()
    except Exception as e:
        logging.error(f"Error in get_filtered_matches: {e}")
        return []
    finally:
        if conn:
            release_db_conn(conn)


def search_teams_and_competitions(query: str) -> List[Dict[str, Any]]:
    """
    Searches for teams and competitions matching the query for clickable results.
    """
    if not query:
        return []

    conn = None
    try:
        conn = get_db_conn()
        with conn.cursor() as cur:
            search_term = f"%{query}%"

            # 1. Team Search
            cur.execute("""
                SELECT 
                    team_id as id, 
                    name, 
                    'team' as type,
                    crest as emblem 
                FROM teams 
                WHERE name ILIKE %s OR short_name ILIKE %s
                LIMIT 10
            """, (search_term, search_term))
            team_results = cur.fetchall()

            # 2. Competition Search
            cur.execute("""
                SELECT 
                    competition_id as id, 
                    name, 
                    'competition' as type,
                    emblem,
                    code
                FROM competitions 
                WHERE name ILIKE %s OR code ILIKE %s
                LIMIT 10
            """, (search_term, search_term))
            comp_results = cur.fetchall()
            
            # 3. Player Search Placeholder
            # NOTE: When a 'players' table is implemented, add the search logic here:
            # player_results = []
            # cur.execute("SELECT player_id as id, name, 'player' as type, photo as emblem FROM players WHERE name ILIKE %s LIMIT 10", (search_term,))
            # player_results = cur.fetchall()
            # return team_results + comp_results + player_results

            return team_results + comp_results

    except Exception as e:
        logging.error(f"Error in search_teams_and_competitions: {e}")
        return []
    finally:
        if conn:
            release_db_conn(conn)