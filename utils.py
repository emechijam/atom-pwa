# utils.py v1.5
#
# WHAT'S NEW (v1.5):
# - BUG FIX (Standings: 0): Changed 'count_table' to count
#   'standings_lists' instead of the legacy 'standings' table.
#   This will fix the "Standings: 0" bug in the sidebar.
# - BUG FIX (Last updated): Added 'st.cache_data.clear()' to
#   'show_last_updated' to aggressively bust the cache and
#   fix the stuck "14 NOV" date.
# - RETAINED: All v1.4 logic for date parsing.

import streamlit as st
import re
import pytz
import logging
from datetime import datetime, timedelta, timezone
import db  # <-- Import the pool
from typing import List, Dict, Any, Tuple

# --- UTC Parsing Helper (FIXED) ---
LAGOS_TZ = pytz.timezone('Africa/Lagos')
DEFAULT_DATE = "01-01-1900"
DEFAULT_TIME = "00:00:00"

def parse_utc_to_gmt1(utc_date_input: Any) -> Tuple[str, str]:
    """
    Parses a UTC ISO date string OR a datetime object
    and converts it to GMT+1 (Lagos) date and time strings.
    Returns (date_str, time_str)
    """
    if not utc_date_input:
        return (DEFAULT_DATE, DEFAULT_TIME)
    try:
        if isinstance(utc_date_input, datetime):
            utc_dt = utc_date_input
        else:
            utc_dt = datetime.fromisoformat(str(utc_date_input).replace("Z", "+00:00"))

        if utc_dt.tzinfo is None:
            utc_dt = utc_dt.replace(tzinfo=timezone.utc)
            
        gmt1_dt = utc_dt.astimezone(LAGOS_TZ)
        date_str = gmt1_dt.strftime("%d-%m-%Y")
        time_str = gmt1_dt.strftime("%H:%M:%S")
        return (date_str, time_str)
    except Exception as e:
        logging.error(f"Failed to parse date '{utc_date_input}': {e}")
        return (DEFAULT_DATE, DEFAULT_TIME)

# --- CACHED DATA PULLS (Corrected in v1.3) ---

@st.cache_data(ttl=60) # Cache for 1 minute
def load_all_match_data() -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    """
    Loads all upcoming and recent matches from the database.
    """
    logging.info("DB: Caching load_all_match_data()")
    
    utc_now = datetime.now(pytz.UTC)
    utc_today = utc_now.date()
    seven_days_ago = utc_today - timedelta(days=7)

    sql = """
    SELECT
        m.match_id,
        m.competition_id,
        m.utc_date,
        m.status,
        m.matchday,
        m.home_team_id,
        m.away_team_id,
        m.score_fulltime_home,
        m.score_fulltime_away,
        m.score_halftime_home,
        m.score_halftime_away,
        m.score_winner,
        m.details_populated,
        m.last_updated,
        m.raw_data,
        p.prediction_data,
        comp.code as competition_code
    FROM
        matches m
    LEFT JOIN
        competitions comp ON m.competition_id = comp.competition_id
    LEFT JOIN
        predictions p ON m.match_id = p.match_id
    WHERE
        m.utc_date >= %s
    ORDER BY
        m.utc_date ASC;
    """
    
    conn = None
    all_matches = []
    try:
        conn = db.db_pool.getconn()
        with conn.cursor() as cur:
            cur.execute(sql, (seven_days_ago,))
            all_matches = cur.fetchall()
            
    except Exception as e:
        st.error(f"Failed to load match data: {e}")
    finally:
        if conn:
            db.db_pool.putconn(conn)
            
    future_matches = []
    past_matches = []
    upcoming_statuses = {'SCHEDULED', 'TIMED', 'TIME', 'POSTPONED'}
    
    for match in all_matches:
        if match['status'] in upcoming_statuses:
            future_matches.append(match)
        else:
            past_matches.append(match)
            
    past_matches.sort(key=lambda m: (m['utc_date'] is not None, m['utc_date']), reverse=True)
            
    return future_matches, past_matches


# --- GENERAL HELPERS (Corrected in v1.3) ---

def count_table(table_name: str, status: List[str] = None) -> int:
    """Counts rows in a given table, optionally filtering by status."""
    conn = None
    
    # --- START v1.5 FIX: Point to correct standings table ---
    if table_name == 'standings':
        table_name = 'standings_lists'
    # --- END v1.5 FIX ---
    
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
    except Exception as e:
        logging.error(f"Failed to count table {table_name}: {e}")
        return 0
    finally:
        if conn:
            db.db_pool.putconn(conn)


def format_date(date_str):
    """
    Formats a date string to 'DD MMM, YYYY' uppercase.
    (Kept for widgets.py)
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


@st.cache_data(ttl=60) # Cache for 1 minute
def show_last_updated():
    """
    Displays the last updated time based on the most recent match.last_updated in DB.
    """
    # --- START v1.5 FIX: Aggressively bust the cache ---
    st.cache_data.clear()
    # --- END v1.5 FIX ---
    
    if st.session_state.get('sync_thread_running'):
        st.sidebar.caption("Last updated: **Updating...**")
        return

    conn = None
    try:
        conn = db.db_pool.getconn()
        with conn.cursor() as cur:
            cur.execute("SELECT MAX(last_updated) as last_update FROM matches")
            latest = cur.fetchone()
            
            if latest and latest['last_update']:
                utc_dt = latest['last_update']
                
                if utc_dt.tzinfo is None:
                    utc_dt = pytz.UTC.localize(utc_dt)
                    
                gmt1_dt = utc_dt.astimezone(LAGOS_TZ)
                updated_date_str = gmt1_dt.strftime("%d %b, %Y").upper()
                updated_time_str = gmt1_dt.strftime("%H:%M:%S %Z")
                st.sidebar.caption(
                    f"Last updated: **{updated_date_str} {updated_time_str}**"
                )
            else:
                st.sidebar.caption("Last updated: **—**")
    except Exception as e:
        logging.error(f"Failed to get last updated time: {e}")
        st.sidebar.caption("Last updated: **Error**")
    finally:
        if conn:
            db.db_pool.putconn(conn)


def get_structured_match_info(match_data, target_team_name):
    """
    Parses match result string into structured information.
    (Kept for widgets.py)
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