# sync.py v3.7 - Hybrid Live Poller (FD + AS) & Predictor Trigger
#
# WHAT'S NEW (v3.7):
# - REALITY CHECK: The AS-API free plan does NOT support 14 days
#   (their AI bot was wrong). The API server-level error confirms
#   it's limited to 1 day past, 1 day future.
# - CONFIG FIX: Reverting AS_DAYS_AHEAD from 14 back to 1.
#   This stops the script from wasting 13 API calls every 6 hours
#   on dates that will always be rejected by the free plan.
#
# WHAT'S NEW (v3.5):
# - JIT POPULATION: Solved the "Skipping AS match" warning.

import os
import time
import pytz
import logging
import threading
import requests
import psycopg2
import datetime
import json
import sys
import subprocess
import re
from datetime import UTC
from psycopg2.pool import ThreadedConnectionPool
from psycopg2.extras import execute_values, RealDictCursor
from dotenv import load_dotenv
from queue import PriorityQueue
from typing import List, Tuple, Dict, Any, Optional
from concurrent.futures import ThreadPoolExecutor

# ============ CONFIG & LOGGING ============
load_dotenv()
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(threadName)s] [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

# --- FD (football-data.org) Polling Config ---
FD_POLL_INTERVAL_MINUTES = 5
FD_DAYS_BEHIND = 2
FD_DAYS_AHEAD = 30
FD_MAX_WORKERS = 5

# --- AS (api-sports.io) Polling Config ---
AS_POLL_INTERVAL_HOURS = 0.25 #Changed to every 15mins
AS_DAYS_BEHIND = 1  # Free plan allows yesterday
AS_DAYS_AHEAD = 1   # <-- v3.7 FIX: Reverted to 1. 14 is not supported.
AS_DAILY_LIMIT = 100 # Global 100/day limit

# --- General Config ---
VERSION = "v3.7" # <-- Updated
MAX_DB_CONNECTIONS = 10

# --- Free Tier Competition Codes (Must match populator.py) ---
FREE_CODES = [
    'BSA', 'BL1', 'CL', 'DED', 'EC', 'FL1', 'PD', 'PL', 'PPL', 'SA', 'WC',
    'ELC', 'CLI'
]

# --- ID Management ---
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

# ============ FD API & KEY ROTATOR ============
FD_BASE_URL = "https://api.football-data.org/v4"
FD_API_KEYS = [
    k.strip()
    for k in os.getenv("FOOTBALL_DATA_API_KEY", "").split(",")
    if k.strip()
]
if not FD_API_KEYS:
    logging.error(
        "FOOTBALL_DATA_API_KEY not found. Check .env file or Streamlit Secrets."
    )
    exit(1)

http_session = requests.Session()


class KeyRotator:
    """Manages FD API keys using a PriorityQueue."""
    def __init__(self, keys: List[str]):
        self.queue = PriorityQueue()
        self.lock = threading.Lock()
        if not keys:
            logging.error("No FD API keys provided to KeyRotator.")
            exit(1)
        for key in keys:
            self.queue.put((0, key))
        logging.info(f"LOADED {len(keys)} FD KEYS — SMART COOLDOWN MODE!")

    def get_next(self) -> str:
        next_free_time, key = self.queue.get()
        now = time.time()
        if next_free_time > now:
            sleep_duration = next_free_time - now
            with self.lock:
                logging.info(
                    f"FD Key {key[:8]}... on cooldown. "
                    f"Sleeping for {sleep_duration:.2f}s"
                )
            time.sleep(sleep_duration)
        with self.lock:
            logging.debug(f"FD KEY → {key[:8]}...")
        return key

    def release(self, key: str):
        next_use = time.time() + 6.5
        self.queue.put((next_use, key))

    def penalize(self, key: str):
        with self.lock:
            logging.warning(f"FD PENALIZED → {key[:8]}... | Cooldown 70s")
        next_use = time.time() + 70.0
        self.queue.put((next_use, key))


fd_rotator = KeyRotator(FD_API_KEYS)


def api_call_fd(endpoint: str, params: Optional[Dict] = None) -> Dict[str, Any]:
    """Makes an API call to football-data.org (FD)"""
    key = ""
    try:
        key = fd_rotator.get_next()
        r = http_session.get(
            f"{FD_BASE_URL}/{endpoint}",
            headers={"X-Auth-Token": key},
            params=params,
            timeout=20,
        )
        r.raise_for_status()
        try:
            data = r.json()
        except requests.exceptions.JSONDecodeError:
            data = {}
        fd_rotator.release(key)
        return data
    except requests.exceptions.HTTPError as http_err:
        if http_err.response.status_code == 400:
            logging.error(f"FD HTTP Error: {http_err} for url: {http_err.response.url}")
        elif http_err.response.status_code == 403:
            logging.warning(f"FD API Call Forbidden (403) for {endpoint}.")
        elif http_err.response.status_code == 429:
            logging.warning(f"FD API Call Rate-Limited (429) for {endpoint}.")
        else:
            logging.error(f"FD HTTP Error: {http_err}")
        if key: fd_rotator.penalize(key)
        raise http_err
    except Exception as e:
        logging.error(f"FD API call to {endpoint} failed: {e}")
        if key: fd_rotator.penalize(key)
        raise e

# ============ AS API (API-SPORTS) CALLER ============
AS_API_KEY = os.getenv("API_SPORTS_KEY")
AS_BASE_URL = "https://v3.football.api-sports.io"
AS_HEADERS = {"x-apisports-key": AS_API_KEY}

# AS API Limit Tracking (Global State)
as_request_count = 0
as_daily_reset_time = time.time() + (24 * 60 * 60)
as_lock = threading.Lock()


def api_call_as(endpoint: str, params: Optional[Dict] = None) -> Dict[str, Any]:
    """
    Makes an API call to api-sports.io (AS).
    Manages both 10/min and 100/day rate limits.
    """
    global as_request_count, as_daily_reset_time
    if not AS_API_KEY:
        raise Exception("API_SPORTS_KEY is not configured.")

    try:
        with as_lock:
            if time.time() > as_daily_reset_time:
                logging.info("API-Sports daily limit counter reset.")
                as_request_count = 0
                as_daily_reset_time = time.time() + (24 * 60 * 60)

            if as_request_count >= AS_DAILY_LIMIT:
                raise Exception(
                    "API-Sports 100 requests/day limit exceeded. "
                    "Try again later."
                )
            as_request_count += 1
            logging.info(
                f"API-Sports request {as_request_count}/{AS_DAILY_LIMIT} "
                "for the day."
            )

        r = http_session.get(
            f"{AS_BASE_URL}{endpoint}",
            headers=AS_HEADERS,
            params=params,
            timeout=20,
        )
        
        # Respect 10/min limit
        time.sleep(6.5)

        r.raise_for_status()
        data = r.json()

        if not data.get("response") and data.get("errors"):
            logging.error(f"AS API Error: {data['errors']}")
            if isinstance(data['errors'], dict) and 'plan' in data['errors']:
                logging.info(f"AS Plan Error: {data['errors']['plan']}")
                return {}
            return {}
        
        return data.get("response", [])

    except Exception as e:
        logging.error(f"AS API call to {endpoint} failed: {e}")
        raise e

# ============ HYBRID UPSERTS & TRANSFORMERS ============

# --- START OF v3.5: JIT POPULATION ---
# These functions are borrowed from build_leagues.py to be used by sync
def _get_or_create_area(cur, area_name: str,
                        country_code: str = None) -> Optional[int]:
    """
    Finds an existing area or creates a new one.
    This is necessary because AS leagues are tied to countries (areas).
    """
    if not area_name:
        logging.warning("Cannot create area with no name. Using NULL.")
        return None
        
    # Try to find area by name
    cur.execute("SELECT area_id FROM areas WHERE name = %s", (area_name,))
    row = cur.fetchone()
    if row:
        return row['area_id']
    
    # Not found, create it.
    cur.execute("SELECT COALESCE(MAX(area_id), 2272) + 1 AS next_id FROM areas")
    next_id = cur.fetchone()['next_id']
    
    try:
        cur.execute(
            """
            INSERT INTO areas (area_id, name, code, flag)
            VALUES (%s, %s, %s, NULL)
            """,
            (next_id, area_name, country_code)
        )
        logging.info(f"JIT: Created new area for {area_name} with ID {next_id}")
        return next_id
    except psycopg2.Error as e:
        logging.error(f"JIT: Failed to create new area {area_name}: {e}")
        cur.connection.rollback() # Rollback this specific INSERT
        return None

def _get_or_create_as_competition(cur, league: dict,
                                  country: dict) -> Optional[int]:
    """
    Finds an existing AS competition or creates a new one *on the fly*.
    """
    as_league_id = league.get('id')
    if not as_league_id:
        return None

    # 1. Check if it exists
    cur.execute(
        "SELECT competition_id FROM competitions WHERE as_competition_id = %s",
        (as_league_id,)
    )
    row = cur.fetchone()
    if row:
        return row['competition_id']
        
    # 2. Not found, create it
    logging.warning(
        f"JIT: Creating new competition: {league.get('name')} "
        f"(AS ID: {as_league_id})"
    )
    
    area_id = _get_or_create_area(cur, country.get('name'), country.get('code'))
    offset_id = as_league_id + AS_ID_OFFSET
    now = datetime.datetime.now(UTC).isoformat()
    
    try:
        cur.execute(
            """
            INSERT INTO competitions (
                competition_id, area_id, name, code, type, emblem,
                last_updated, as_competition_id
            )
            VALUES (%s, %s, %s, NULL, %s, %s, %s, %s)
            """,
            (
                offset_id, area_id, league.get('name'), league.get('type'),
                league.get('logo'), now, as_league_id
            )
        )
        return offset_id
    except psycopg2.Error as e:
        logging.error(f"JIT: Failed to create competition {league.get('name')}: {e}")
        cur.connection.rollback()
        return None
# --- END OF v3.5: JIT POPULATION ---


def upsert_teams(cur, teams_data: List[Dict]):
    """
    Upserts a list of teams (FD format).
    """
    if not teams_data:
        return
    sql = """
    INSERT INTO teams (team_id, area_id, name, short_name, tla, crest,
                       address, website, founded, club_colors, venue,
                       last_updated, as_team_id)
    VALUES %s
    ON CONFLICT (team_id) DO UPDATE SET
        area_id = EXCLUDED.area_id,
        name = EXCLUDED.name,
        short_name = EXCLUDED.short_name,
        tla = EXCLUDED.tla,
        crest = EXCLUDED.crest,
        address = EXCLUDED.address,
        website = EXCLUDED.website,
        founded = EXCLUDED.founded,
        club_colors = EXCLUDED.club_colors,
        venue = EXCLUDED.venue,
        last_updated = EXCLUDED.last_updated,
        as_team_id = EXCLUDED.as_team_id;
    """
    unique_teams = {}
    for t in teams_data:
        if t and t.get('id'):
            unique_teams[t['id']] = t

    values = [
        (
            t["id"],
            t.get("area", {}).get("id"),
            t.get("name"),
            t.get("shortName"),
            t.get("tla"),
            t.get("crest"),
            t.get("address"),
            t.get("website"),
            t.get("founded"),
            t.get("clubColors"),
            t.get("venue"),
            t.get("lastUpdated"),
            t.get("as_team_id")
        )
        for t in unique_teams.values()
    ]
    if values:
        execute_values(cur, sql, values, page_size=100)


def upsert_matches_from_fd_sync(cur, matches_data: List[Dict]):
    """
    Upserts a list of basic match info from the FD /matches endpoint.
    """
    if not matches_data:
        return 0
    sql = """
    INSERT INTO matches (match_id, competition_id, season_year, utc_date, status,
                         matchday, stage, group_name, home_team_id,
                         away_team_id, score_winner, score_duration,
                         score_fulltime_home, score_fulltime_away,
                         score_halftime_home, score_halftime_away,
                         last_updated, raw_data, details_populated)
    VALUES %s
    ON CONFLICT (match_id) DO UPDATE SET
        competition_id = EXCLUDED.competition_id,
        season_year = EXCLUDED.season_year,
        utc_date = EXCLUDED.utc_date,
        status = EXCLUDED.status,
        matchday = EXCLUDED.matchday,
        stage = EXCLUDED.stage,
        group_name = EXCLUDED.group_name,
        home_team_id = EXCLUDED.home_team_id,
        away_team_id = EXCLUDED.away_team_id,
        score_winner = EXCLUDED.score_winner,
        score_duration = EXCLUDED.score_duration,
        score_fulltime_home = EXCLUDED.score_fulltime_home,
        score_fulltime_away = EXCLUDED.score_fulltime_away,
        score_halftime_home = EXCLUDED.score_halftime_home,
        score_halftime_away = EXCLUDED.score_halftime_away,
        last_updated = EXCLUDED.last_updated,
        raw_data = EXCLUDED.raw_data,
        details_populated = FALSE;
    """
    values = []
    teams_to_upsert = []
    for m in matches_data:
        if not m.get('id'):
            continue

        competition = m.get("competition", {})
        season = m.get("season", {})
        score = m.get("score", {})
        fullTime = score.get("fullTime", {})
        halfTime = score.get("halfTime", {})
        home_team = m.get("homeTeam", {})
        away_team = m.get("awayTeam", {})

        season_year_str = season.get("startDate", "1900-01-01")[:4]
        try:
            season_year = int(season_year_str)
        except ValueError:
            season_year = 1900

        if home_team and home_team.get('id'):
            teams_to_upsert.append(home_team)
        if away_team and away_team.get('id'):
            teams_to_upsert.append(away_team)

        values.append((
            m["id"],
            competition.get("id"),
            season_year,
            m.get("utcDate"),
            m.get("status"),
            m.get("matchday"),
            m.get("stage"),
            m.get("group"),
            home_team.get("id") if home_team else None,
            away_team.get("id") if away_team else None,
            score.get("winner"),
            score.get("duration"),
            fullTime.get("home"),
            fullTime.get("away"),
            halfTime.get("home"),
            halfTime.get("away"),
            m.get("lastUpdated"),
            json.dumps(m),
            False
        ))
    if values:
        upsert_teams(cur, teams_to_upsert)
        execute_values(cur, sql, values, page_size=100)
    return len(values)


def _parse_as_matchday(round_str: Optional[str]) -> Optional[int]:
    """
    Safely parses the 'round' string from AS API.
    It can be "Regular Season - 38" or "Semi-finals".
    """
    if not round_str:
        return None
    try:
        # Use regex to find the last number in the string
        matches = re.findall(r'\d+', round_str)
        if matches:
            return int(matches[-1])
        return None # "Semi-finals" has no number
    except (ValueError, TypeError):
        logging.debug(f"Could not parse matchday from '{round_str}'. Storing NULL.")
        return None


def upsert_data_from_as_sync(cur, as_matches: List[Dict],
                             fd_as_league_ids_to_skip: set):
    """
    Transforms and upserts AS fixture data, skipping FD-covered leagues.
    """
    if not as_matches:
        return 0
    
    matches_to_insert = []
    teams_to_insert = []
    
    # 1. Get all comp IDs from DB (with offset)
    cur.execute(
        "SELECT competition_id, as_competition_id FROM competitions "
        "WHERE as_competition_id IS NOT NULL"
    )
    # Map AS ID -> Offset DB ID
    as_comp_map = {
        row['as_competition_id']: row['competition_id'] for row in cur.fetchall()
    }

    for m in as_matches:
        league = m.get("league", {})
        country = m.get("country", {})
        as_league_id = league.get("id")
        
        # 1. DE-DUPLICATION
        if as_league_id in fd_as_league_ids_to_skip:
            continue
            
        # 2. Find offset Competition ID
        competition_id_offset = as_comp_map.get(as_league_id)
        
        # --- START OF v3.5: JIT LOGIC ---
        if not competition_id_offset:
            # This is the JIT Populator
            # Create the competition and get its new offset ID
            new_offset_id = _get_or_create_as_competition(cur, league, country)
            
            if new_offset_id:
                competition_id_offset = new_offset_id
                # Add to map so we don't re-create it in this batch
                as_comp_map[as_league_id] = new_offset_id 
            else:
                logging.error(
                    f"Failed to JIT-create league {league.get('name')}. "
                    "Skipping match."
                )
                continue # Skip this match if creation failed
        # --- END OF v3.5: JIT LOGIC ---
            
        # 3. TRANSFORM TEAMS
        fixture = m.get("fixture", {})
        teams = m.get("teams", {})
        venue = fixture.get("venue", {})
        
        home_team = teams.get("home", {})
        away_team = teams.get("away", {})
        
        if home_team.get("id"):
            teams_to_insert.append({
                "id": home_team["id"] + AS_ID_OFFSET,
                "as_team_id": home_team["id"],
                "name": home_team.get("name"),
                "tla": None,
                "crest": home_team.get("logo"),
                "venue": venue.get("name"),
                "lastUpdated": datetime.datetime.now(pytz.UTC).isoformat()
            })
        if away_team.get("id"):
            teams_to_insert.append({
                "id": away_team["id"] + AS_ID_OFFSET,
                "as_team_id": away_team["id"],
                "name": away_team.get("name"),
                "tla": None,
                "crest": away_team.get("logo"),
                "venue": venue.get("name"),
                "lastUpdated": datetime.datetime.now(pytz.UTC).isoformat()
            })

        # 4. TRANSFORM MATCH
        goals = m.get("goals", {})
        score = m.get("score", {})
        status_map = {
            'TBD': 'SCHEDULED', 'NS': 'SCHEDULED', '1H': 'IN_PLAY',
            'HT': 'IN_PLAY', '2H': 'IN_PLAY', 'ET': 'IN_PLAY',
            'P': 'IN_PLAY', 'FT': 'FINISHED', 'AET': 'FINISHED',
            'PEN': 'FINISHED', 'BT': 'IN_PLAY', 'SUSP': 'PAUSED',
            'INT': 'PAUSED', 'PST': 'POSTPONED', 'CANC': 'CANCELED',
            'ABD': 'CANCELED', 'AWD': 'FINISHED', 'WO': 'FINISHED'
        }
        status = status_map.get(fixture.get("status", {}).get("short"), 'SCHEDULED')
        
        winner_map = {True: 'HOME_TEAM', False: 'AWAY_TEAM', None: 'DRAW'}
        winner = winner_map.get(teams.get("home", {}).get("winner"))

        as_season_year = m.get("league", {}).get("season")
        if not as_season_year:
             try:
                as_season_year = datetime.datetime.fromisoformat(
                     fixture.get("date")
                ).year
             except:
                as_season_year = datetime.datetime.now().year # Fallback

        matches_to_insert.append({
            "id": fixture["id"] + AS_ID_OFFSET,
            "competition_id": competition_id_offset,
            "season_year": as_season_year,
            "utcDate": fixture.get("date"),
            "status": status,
            "matchday": _parse_as_matchday(m.get("league", {}).get("round")),
            "group": m.get("league", {}).get("group"),
            "homeTeam": {"id": home_team.get("id", 0) + AS_ID_OFFSET},
            "awayTeam": {"id": away_team.get("id", 0) + AS_ID_OFFSET},
            "score": {
                "winner": winner,
                "duration": "REGULAR",
                "fullTime": {"home": goals.get("home"), "away": goals.get("away")},
                "halfTime": {"home": score.get("halftime", {}).get("home"),
                             "away": score.get("halftime", {}).get("away")}
            },
            "lastUpdated": datetime.datetime.fromtimestamp(
                fixture.get("timestamp", 0), pytz.UTC
            ).isoformat(),
            "raw_data": json.dumps(m)
        })

    # 5. UPSERT BATCH
    if teams_to_insert:
        upsert_teams(cur, teams_to_insert)
        
    if not matches_to_insert:
        return 0
        
    sql = """
    INSERT INTO matches (match_id, competition_id, season_year, utc_date, status,
                         matchday, stage, group_name, home_team_id,
                         away_team_id, score_winner, score_duration,
                         score_fulltime_home, score_fulltime_away,
                         score_halftime_home, score_halftime_away,
                         last_updated, raw_data, details_populated)
    VALUES %s
    ON CONFLICT (match_id) DO UPDATE SET
        competition_id = EXCLUDED.competition_id,
        season_year = EXCLUDED.season_year,
        utc_date = EXCLUDED.utc_date,
        status = EXCLUDED.status,
        matchday = EXCLUDED.matchday,
        stage = EXCLUDED.stage,
        group_name = EXCLUDED.group_name,
        home_team_id = EXCLUDED.home_team_id,
        away_team_id = EXCLUDED.away_team_id,
        score_winner = EXCLUDED.score_winner,
        score_duration = EXCLUDED.score_duration,
        score_fulltime_home = EXCLUDED.score_fulltime_home,
        score_fulltime_away = EXCLUDED.score_fulltime_away,
        score_halftime_home = EXCLUDED.score_halftime_home,
        score_halftime_away = EXCLUDED.score_halftime_away,
        last_updated = EXCLUDED.last_updated,
        raw_data = EXCLUDED.raw_data,
        details_populated = FALSE;
    """
    
    values = []
    for m in matches_to_insert:
        score = m.get("score", {})
        fullTime = score.get("fullTime", {})
        halfTime = score.get("halfTime", {})
        
        values.append((
            m["id"],
            m["competition_id"],
            m["season_year"],
            m.get("utcDate"),
            m.get("status"),
            m.get("matchday"),
            m.get("stage"),
            m.get("group"),
            m.get("homeTeam", {}).get("id"),
            m.get("awayTeam", {}).get("id"),
            score.get("winner"),
            score.get("duration"),
            fullTime.get("home"),
            fullTime.get("away"),
            halfTime.get("home"),
            halfTime.get("away"),
            m.get("lastUpdated"),
            m.get("raw_data"),
            False
        ))

    if values:
        execute_values(cur, sql, values, page_size=100)
    return len(values)


# ============ POLLING FUNCTIONS ============

def get_fd_competition_id_map() -> Dict[str, int]:
    """
    Queries the local database to map FREE_CODES (e.g., 'PL')
    to their API competition_id (e.g., 2021).
    """
    logging.info("Fetching FD competition ID map from local database...")
    conn = None
    try:
        conn = db_pool.getconn()
        with conn.cursor() as cur:
            codes_tuple = tuple(FREE_CODES)
            cur.execute(
                "SELECT code, competition_id FROM competitions WHERE code IN %s",
                (codes_tuple,)
            )
            rows = cur.fetchall()
            
            comp_map = {row['code']: row['competition_id'] for row in rows}
            
            if len(comp_map) < len(FREE_CODES):
                logging.warning(
                    "Not all free codes were found in the database. "
                    "Run populator.py or build_leagues.py first."
                )
            
            logging.info(f"Found {len(comp_map)} FD competitions to poll.")
            return comp_map
            
    except Exception as e:
        logging.error(f"Failed to get FD competition ID map: {e}")
        return {}
    finally:
        if conn:
            db_pool.putconn(conn)


def get_as_leagues_to_skip() -> set:
    """
    Gets the set of AS league IDs that we should SKIP
    because they are covered by the FD poller.
    """
    logging.info("Fetching AS league ID skip-list...")
    conn = None
    try:
        conn = db_pool.getconn()
        with conn.cursor() as cur:
            codes_tuple = tuple(FREE_CODES)
            cur.execute(
                "SELECT as_competition_id FROM competitions "
                "WHERE code IN %s AND as_competition_id IS NOT NULL",
                (codes_tuple,)
            )
            # Return a set of AS IDs, e.g., {39, 2, 78, ...}
            return {row['as_competition_id'] for row in cur.fetchall()}
    except Exception as e:
        logging.error(f"Failed to get AS skip list: {e}")
        return set() # Return empty set on failure
    finally:
        if conn:
            db_pool.putconn(conn)


def poll_fd_competition(comp_code: str, comp_id: int,
                        date_from: str, date_to: str) -> int:
    """
    Worker function to fetch and sync matches for a *single* FD competition.
    """
    conn = None
    try:
        logging.info(f"FD Polling: {comp_code} (ID: {comp_id})...")
        params = {"dateFrom": date_from, "dateTo": date_to}
        endpoint = f"competitions/{comp_id}/matches"
        
        response = api_call_fd(endpoint, params=params)
        matches = response.get("matches", [])
        
        if not matches:
            logging.info(f"No FD matches found for {comp_code}.")
            return 0

        conn = db_pool.getconn()
        with conn.cursor() as cur:
            count = upsert_matches_from_fd_sync(cur, matches)
            conn.commit()
            logging.info(
                f"Successfully upserted {count} FD matches for {comp_code}."
            )
            return count
            
    except Exception as e:
        logging.error(f"Failed to poll FD competition {comp_code}.")
        if conn: conn.rollback()
        return 0
    finally:
        if conn: db_pool.putconn(conn)


def poll_as_fixtures(date_from: str, date_to: str,
                     fd_as_ids_to_skip: set) -> int:
    """
    Polls AS for all fixtures in the date range, transforms, and upserts them.
    """
    if not AS_API_KEY:
        logging.info("AS Polling: Skipping, API_SPORTS_KEY not set.")
        return 0
        
    logging.info(
        f"--- STARTING API-Sports Sync ({date_from} to {date_to}) ---"
    )
    total_matches_synced = 0
    conn = None
    
    try:
        date_from_obj = datetime.datetime.strptime(
            date_from, "%Y-%m-%d"
        ).date()
        date_to_obj = datetime.datetime.strptime(date_to, "%Y-%m-%d").date()
        
        current_date = date_from_obj
        while current_date <= date_to_obj:
            date_str = current_date.strftime("%Y-%m-%d")
            logging.info(f"AS Polling: Fetching fixtures for {date_str}...")
            
            try:
                # 1. API Call
                as_matches = api_call_as(
                    "/fixtures", {"date": date_str}
                )
                if not as_matches:
                    logging.info(f"No AS matches found for {date_str}.")
                    current_date += datetime.timedelta(days=1)
                    continue

                # 2. Upsert
                conn = db_pool.getconn()
                with conn.cursor() as cur:
                    count = upsert_data_from_as_sync(
                        cur, as_matches, fd_as_ids_to_skip
                    )
                    conn.commit()
                
                logging.info(
                    f"Successfully upserted {count} new AS matches for {date_str}."
                )
                total_matches_synced += count

            except Exception as e:
                logging.error(f"Failed to process AS date {date_str}: {e}")
                if "limit" in str(e):
                    logging.CRITICAL(
                        "AS daily limit hit during sync. "
                        "Stopping AS sync cycle."
                    )
                    break
            finally:
                if conn:
                    db_pool.putconn(conn)
                    conn = None

            current_date += datetime.timedelta(days=1)

        logging.info(
            f"--- FINISHED API-Sports Sync. {total_matches_synced} matches. ---"
        )
        return total_matches_synced

    except Exception as e:
        logging.error(f"AS Polling cycle failed: {e}")
        return total_matches_synced


# ============ MAIN POLLING LOOP ============
def main():
    logging.info(f"--- SYNC POLLER ({VERSION}) STARTING ---")
    
    # --- Initial Data Fetch ---
    fd_comp_map = get_fd_competition_id_map()
    as_leagues_to_skip = get_as_leagues_to_skip()
    
    if not fd_comp_map:
        logging.error("No FD competitions to poll. Run build_leagues.py first.")
    if not as_leagues_to_skip and AS_API_KEY:
         logging.warning(
             "No AS skip list found. AS poller may create duplicates."
         )

    last_as_poll_time = 0.0

    while True:
        cycle_start_time = time.time()
        total_matches_synced = 0
        run_predictor = False
        
        try:
            # === 1. FD Poller (5-minute interval) ===
            logging.info("--- STARTING FD Sync Cycle ---")
            today = datetime.datetime.now(UTC)
            fd_date_from = (
                today - datetime.timedelta(days=FD_DAYS_BEHIND)
            ).strftime("%Y-%m-%d")
            fd_date_to = (
                today + datetime.timedelta(days=FD_DAYS_AHEAD)
            ).strftime("%Y-%m-%d")
            
            logging.info(f"FD Polling: {fd_date_from} to {fd_date_to} "
                         f"for {len(fd_comp_map)} competitions...")
            
            with ThreadPoolExecutor(
                max_workers=FD_MAX_WORKERS, thread_name_prefix="FDSyncWorker"
            ) as executor:
                futures = {
                    executor.submit(
                        poll_fd_competition, code, comp_id,
                        fd_date_from, fd_date_to
                    ): code
                    for code, comp_id in fd_comp_map.items()
                }
                for future in futures:
                    try:
                        matches_count = future.result()
                        total_matches_synced += matches_count
                    except Exception as e:
                        pass

            if total_matches_synced > 0:
                run_predictor = True

            # === 2. AS Poller (6-hour interval) ===
            time_since_last_as_poll = time.time() - last_as_poll_time
            if time_since_last_as_poll > (AS_POLL_INTERVAL_HOURS * 60 * 60):
                logging.info(
                    f"--- {AS_POLL_INTERVAL_HOURS}h passed. "
                    "Running AS Sync Cycle. ---"
                )
                as_date_from = (
                    today - datetime.timedelta(days=AS_DAYS_BEHIND)
                ).strftime("%Y-%m-%d")
                as_date_to = (
                    today + datetime.timedelta(days=AS_DAYS_AHEAD)
                ).strftime("%Y-%m-%d")
                
                as_matches_count = poll_as_fixtures(
                    as_date_from, as_date_to, as_leagues_to_skip
                )
                
                total_matches_synced += as_matches_count
                if as_matches_count > 0:
                    run_predictor = True
                last_as_poll_time = time.time()
            else:
                logging.info(
                    "Skipping AS sync (not time yet). "
                    f"Next run in approx. "
                    f"{( (AS_POLL_INTERVAL_HOURS * 3600) - time_since_last_as_poll ) / 60:.0f} "
                    "minutes."
                )

            # === 3. Run Predictor (if needed) ===
            if run_predictor:
                logging.info(
                    f"Successfully synced {total_matches_synced} total matches. "
                    "Triggering predictor.py..."
                )
                try:
                    result = subprocess.run(
                        [sys.executable, "predictor.py"],
                        check=True,
                        capture_output=True,
                        text=True,
                        encoding=sys.stdout.encoding,
                        errors='replace'
                    )
                    logging.info("predictor.py finished successfully.")
                except subprocess.CalledProcessError as e:
                    logging.error(f"predictor.py failed: {e.returncode}")
                    logging.error(f"PREDICTOR STDOUT: {e.stdout}")
                    logging.error(f"PREDICTOR STDERR: {e.stderr}")
                except FileNotFoundError:
                    logging.error("ERROR: predictor.py not found.")
            else:
                logging.info("No new/updated matches found. Predictor not needed.")

        except Exception as e:
            logging.error(f"Main poll cycle failed: {e}")
        finally:
            cycle_end_time = time.time()
            logging.info(
                f"Poll cycle finished in {cycle_end_time - cycle_start_time:.2f}s."
            )
            
            sleep_duration = (FD_POLL_INTERVAL_MINUTES * 60) - (
                cycle_end_time - cycle_start_time
            )
            if sleep_duration < 0:
                sleep_duration = 0
                
            logging.info(
                f"Sleeping for {sleep_duration:.2f} seconds "
                f"(until next 5-min interval)..."
            )
            time.sleep(sleep_duration)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        logging.info("--- SYNC POLLER STOPPING (KeyboardInterrupt) ---")
    except Exception as e:
        logging.error(f"--- SYNC POLLER CRASHED: {e} ---")
    finally:
        if 'db_pool' in globals():
            db_pool.closeall()
            logging.info("Database connection pool closed.")