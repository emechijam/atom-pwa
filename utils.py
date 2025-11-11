# utils.py v1.1
# Updated to use psycopg2.pool and raw SQL.
# Added UTC parsing helper.
# FIX (v1.1): Changed invalid (T1, T2) type hints to Tuple[T1, T2]
#             to resolve Pylance 'reportInvalidTypeForm' warnings.

import streamlit as st
import re
import pytz
from datetime import datetime, timedelta, timezone
import db  # <-- Import the pool
from typing import List, Dict, Any, Tuple # <-- FIX 1: Added Tuple

# --- NEW: UTC Parsing Helper ---
LAGOS_TZ = pytz.timezone('Africa/Lagos')
DEFAULT_DATE = "01-01-1900"
DEFAULT_TIME = "00:00:00"

def parse_utc_to_gmt1(utc_date_str: str) -> Tuple[str, str]: # <-- FIX 2
    """
    Parses a UTC ISO date string and converts it to
    GMT+1 (Lagos) date and time strings.
    Returns (date_str, time_str)
    """
    if not utc_date_str:
        return (DEFAULT_DATE, DEFAULT_TIME)
    try:
        # 1. Parse the ISO string (e.g., "2025-11-20T18:00:00Z")
        utc_dt = datetime.fromisoformat(utc_date_str.replace("Z", "+00:00"))
        # 2. Ensure it's timezone-aware (as UTC)
        utc_dt = utc_dt.replace(tzinfo=timezone.utc)
        # 3. Convert to Lagos time
        gmt1_dt = utc_dt.astimezone(LAGOS_TZ)
        # 4. Format to the strings the UI expects
        date_str = gmt1_dt.strftime("%d-%m-%Y")
        time_str = gmt1_dt.strftime("%H:%M:%S")
        return (date_str, time_str)
    except Exception:
        return (DEFAULT_DATE, DEFAULT_TIME)

# --- CACHED DATA PULLS (Rewritten for SQL) ---

@st.cache_data(ttl=timedelta(minutes=5))
def load_all_match_data() -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]: # <-- FIX 3
    """
    Loads all future and past matches from the database using raw SQL
    and LEFT JOINs predictions.
    Returns (future_matches, past_matches) as lists of dicts.
    """
    conn = None
    future = []
    past = []
    
    # Base query
    sql_base = """
    SELECT m.*, p.prediction_data
    FROM matches m
    LEFT JOIN predictions p ON m.match_id = p.match_id
    """
    
    try:
        conn = db.db_pool.getconn()
        
        # --- Future Matches ---
        sql_future = sql_base + """
        WHERE m.status IN ('SCHEDULED', 'TIMED')
        ORDER BY m.utc_date ASC;
        """
        with conn.cursor() as cur:
            cur.execute(sql_future)
            future = cur.fetchall()

        # --- Past Matches ---
        sql_past = sql_base + """
        WHERE m.status IN ('FINISHED', 'IN_PLAY', 'PAUSED')
        ORDER BY m.utc_date DESC;
        """
        with conn.cursor() as cur:
            cur.execute(sql_past)
            past = cur.fetchall()
            
    except Exception as e:
        st.error(f"Failed to load match data: {e}")
    finally:
        if conn:
            db.db_pool.putconn(conn)
            
    # Return lists of dicts (RealDictRow)
    return future, past

# --- GENERAL HELPERS (Rewritten for SQL) ---

def count_table(table_name: str, status: List[str] = None) -> int:
    """Counts rows in a given table, optionally filtering by status."""
    conn = None
    try:
        conn = db.db_pool.getconn()
        with conn.cursor() as cur:
            query = f"SELECT COUNT(*) FROM {table_name}"
            params = []
            
            if status and table_name == 'matches':
                query += " WHERE status = ANY(%s)"
                params.append(status)
                
            cur.execute(query, tuple(params))
            return cur.fetchone()['count']
    except Exception:
        return 0
    finally:
        if conn:
            db.db_pool.putconn(conn)


def format_date(date_str):
    """
    Formats a date string to 'DD MMM, YYYY' uppercase.
    (No database logic, no changes needed)
    """
    try:
        if " " in date_str:
            date_part = date_str.split(" ")[0]
        else:
            date_part = date_str
        dt_obj = datetime.strptime(date_part, "%d-%m-%Y")
        return dt_obj.strftime("%d %b, %Y").upper()
    except Exception:
        return date_str


def show_last_updated():
    """
    Displays the last updated time based on the most recent match.last_updated in DB.
    (Rewritten for SQL)
    """
    if st.session_state.get('sync_thread_running'):
        st.sidebar.caption("Last updated: **Updating...**")
        return

    conn = None
    try:
        conn = db.db_pool.getconn()
        with conn.cursor() as cur:
            cur.execute("SELECT last_updated FROM matches ORDER BY last_updated DESC LIMIT 1")
            latest = cur.fetchone()
            
            if latest and latest['last_updated']:
                utc_dt = latest['last_updated'].replace(tzinfo=pytz.utc) # Ensure tzinfo
                gmt1_dt = utc_dt.astimezone(LAGOS_TZ)
                updated_date_str = gmt1_dt.strftime("%d %b, %Y").upper()
                updated_time_str = gmt1_dt.strftime("%H:%M:%S GMT+1")
                st.sidebar.caption(
                    f"Last updated: **{updated_date_str} {updated_time_str}**"
                )
            else:
                st.sidebar.caption("Last updated: **—**")
    except Exception:
        st.sidebar.caption("Last updated: **—**")
    finally:
        if conn:
            db.db_pool.putconn(conn)


def get_structured_match_info(match_data, target_team_name):
    """
    Parses match result string into structured information.
    (No database logic, no changes needed)
    """
    result = match_data.get("result", "")
    match = re.match(r"(.+?)\s*(\d+)-(\d+)\s*(.+)", result)

    info = {
        "team1_name": "?",
        "team1_score": 0,
        "team2_name": "?",
        "team2_score": 0,
        "is_win": False,
        "is_loss": False,
        "is_draw": False,
        "extra_note": "",
        "target_is_team1": False,
        "target_is_team2": False,
    }

    if not match:
        return info

    team1_name, s1, s2, team2_name = match.groups()
    score1, score2 = int(s1), int(s2)
    is_draw = score1 == score2
    target_is_team1 = team1_name.strip() == target_team_name
    target_is_team2 = team2_name.strip() == target_team_name

    is_win = (target_is_team1 and score1 > score2) or (
        target_is_team2 and score2 > score1
    )
    is_loss = (target_is_team1 and score1 < score2) or (
        target_is_team2 and score2 < score1
    )
    extra_note = " <small>(AP)</small>" if "AP" in result else ""

    info.update(
        {
            "team1_name": team1_name.strip(),
            "team1_score": score1,
            "team2_name": team2_name.strip(),
            "team2_score": score2,
            "is_win": is_win,
            "is_loss": is_loss,
            "is_draw": is_draw,
            "extra_note": extra_note,
            "target_is_team1": target_is_team1,
            "target_is_team2": target_is_team2,
        }
    )
    return info