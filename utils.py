# utils.py v1.9
#
# WHAT'S NEW (v1.9 - DATE PARSE FIX ALIGNMENT):
# - CONFIRMED: `parse_utc_to_gmt1` outputs date string in YYYY-MM-DD format,
#   which is now correctly consumed by app.py v1.14.

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
            # Assume it's an ISO string or similar
            utc_dt = datetime.fromisoformat(str(utc_date_input).replace('Z', '+00:00'))
        
        # Ensure UTC timezone awareness if it was a naive datetime
        if utc_dt.tzinfo is None or utc_dt.tzinfo.utcoffset(utc_dt) is None:
            utc_dt = utc_dt.replace(tzinfo=pytz.utc)

        # Convert to Lagos time (GMT+1)
        lagos_dt = utc_dt.astimezone(LAGOS_TZ)
        
        # Output format is YYYY-MM-DD
        date_str = lagos_dt.strftime("%Y-%m-%d")
        time_str = lagos_dt.strftime("%H:%M:%S")
        return (date_str, time_str)
    except Exception as e:
        # NOTE: This logging is redundant if the calling function logs, but kept for safety.
        # logging.error(f"Error parsing date {utc_date_input}: {e}")
        return (DEFAULT_DATE, DEFAULT_TIME)

def format_date(date_str: str) -> str:
    """Formats date from YYYY-MM-DD to DD Mon YYYY."""
    if not date_str or date_str == DEFAULT_DATE:
        return ""
    try:
        # Input format is YYYY-MM-DD, output format is DD Mon YYYY
        dt_obj = datetime.strptime(date_str, "%Y-%m-%d")
        return dt_obj.strftime("%d %b %Y")
    except ValueError:
        return date_str # Return original if parsing fails

def get_utc_date_range(local_date: datetime) -> Tuple[datetime, datetime]:
    """
    Converts a Streamlit selected date (naive datetime in app's local context, assumed GMT+1)
    to a UTC start and end range for database querying.
    Returns (start_of_day_utc, end_of_day_utc).
    """
    # 1. Localize the input date to the app's assumed local timezone (GMT+1 / Lagos)
    local_start_dt_naive = datetime(local_date.year, local_date.month, local_date.day)
    local_start_dt_aware = LAGOS_TZ.localize(local_start_dt_naive)
    
    # 2. Calculate the end of the day (1ms before next day)
    local_end_dt_aware = local_start_dt_aware + timedelta(days=1) - timedelta(microseconds=1)
    
    # 3. Convert both to UTC
    utc_start_dt = local_start_dt_aware.astimezone(timezone.utc)
    utc_end_dt = local_end_dt_aware.astimezone(timezone.utc)
    
    return (utc_start_dt, utc_end_dt)

def get_structured_match_info(match_data: Dict[str, Any], target_team_name: str) -> Dict[str, Any]:
    """
    Parses a match result string and determines win/loss/draw relative to the target team.
    """
    result = match_data.get("result", "")
    # Pattern: Team1 Name Score1-Score2 Team2 Name (using non-greedy matching for names)
    match = re.match(r"(.+?)\s*(\d+)-(\d+)\s*(.+)", result) 

    info = {
        "team1_name": "?",
        "team1_score": 0,
        "team2_name": "?",
        "team2_score": 0,
        "is_win": False,
        "is_loss": False,
        "is_draw": False,
        "competition": match_data.get("competition", "N/A"),
        "extra_note": "",
        "target_is_team1": False,
        "target_is_team2": False,
    }

    if not match:
        return info

    team1_name, s1, s2, team2_name = match.groups()
    
    # Strip whitespace from names for clean comparison
    team1_name = team1_name.strip()
    team2_name = team2_name.strip()
    
    score1, score2 = int(s1), int(s2)
    
    is_draw = score1 == score2
    target_is_team1 = team1_name == target_team_name
    target_is_team2 = team2_name == target_team_name
    
    is_win = (target_is_team1 and score1 > score2) or \
             (target_is_team2 and score2 > score1)
    
    is_loss = (target_is_team1 and score1 < score2) or \
              (target_is_team2 and score2 < score1)

    info.update({
        "team1_name": team1_name,
        "team1_score": score1,
        "team2_name": team2_name,
        "team2_score": score2,
        "is_win": is_win,
        "is_loss": is_loss,
        "is_draw": is_draw,
        "target_is_team1": target_is_team1,
        "target_is_team2": target_is_team2,
    })

    return info