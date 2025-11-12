# populator.py v8.1
#
# WHAT'S NEW (v8.1):
# - BUG FIX: Patched 'get_or_create_as_area' function.
#   - It no longer crashes on `null value in column "name"`.
#   - If a country name is null, it tries to use the country
#     code, and if that is also null, it defaults to
#     'Unknown Area' instead of crashing the script.

import os
import re
import time
import pytz
import logging
import threading
import requests
import psycopg2
import datetime
import json
import sys
from psycopg2.pool import ThreadedConnectionPool
from psycopg2.extras import execute_values
from dotenv import load_dotenv
from concurrent.futures import ThreadPoolExecutor
from queue import PriorityQueue
from typing import List, Tuple, Dict, Any, Optional

# ============ CONFIG & LOGGING ============
load_dotenv()
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(threadName)s] [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

# --- Year Range for Backfill ---
# FD range
FD_START_YEAR = 2015
FD_END_YEAR = 2025
# AS range (as requested)
AS_SEASONS = [2023, 2022, 2021]
# -------------------------------

# --- Free Tier Competition Codes (FD) ---
FREE_CODES = [
    'BSA', 'BL1', 'CL', 'DED', 'EC', 'FL1', 'PD', 'PL', 'PPL', 'SA', 'WC',
    'ELC', 'CLI'
]
# -----------------------------------

# Concurrency & DB
MAX_WORKERS = 15
MAX_DB_CONNECTIONS = 20
RETRY_SLEEP_SECONDS = 60
VERSION = "v8.1" # <-- Updated

# --- ID Management ---
# Add this offset to all AS IDs to prevent collisions with FD IDs
AS_ID_OFFSET = 1_000_000

# ============ API-SPORTS.IO (AS) CONFIG ============
AS_API_KEY = os.getenv("API_SPORTS_KEY")
AS_BASE_URL = "https://v3.football.api-sports.io"
AS_HEADERS = {"x-apisports-key": AS_API_KEY}
if not AS_API_KEY:
    logging.warning(
        "API_SPORTS_KEY not found. Script will only populate "
        "from football-data.org."
    )

# AS API Limit Tracking (Global State)
as_request_count = 0
as_daily_reset_time = time.time() + (24 * 60 * 60)
as_lock = threading.Lock()

# Manual map of FD codes to their corresponding AS League ID
# This is CRITICAL for de-duplication
FD_AS_LEAGUE_MAP = {
    'PL': 39,    # England: Premier League
    'CL': 2,     # Europe: UEFA Champions League
    'BL1': 78,   # Germany: Bundesliga
    'SA': 135,   # Italy: Serie A
    'PD': 140,   # Spain: La Liga
    'FL1': 61,   # France: Ligue 1
    'DED': 88,   # Netherlands: Eredivisie
    'PPL': 218,  # Portugal: Primeira Liga (Note: AS ID 94 is also PPL)
    'BSA': 390,  # Brazil: Serie A (Note: AS ID 71 is also Serie A)
    'WC': 1,     # World: World Cup
    'EC': 4,     # Europe: European Championship
    'ELC': 40,   # England: Championship
    'CLI': 13,   # South America: Copa Libertadores
}


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
        minconn=2,
        maxconn=MAX_DB_CONNECTIONS,
        dsn=db_url,
    )
    logging.info(
        f"Database connection pool created (Max: {MAX_DB_CONNECTIONS})."
    )
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
        """Gets the soonest-available key, waiting if necessary."""
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
        """A successful call. Put key back on cooldown for 6.5s."""
        next_use = time.time() + 6.5
        self.queue.put((next_use, key))

    def penalize(self, key: str):
        """A rate-limit (429) or other major error occurred."""
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
        if r.status_code == 403:
            logging.warning(
                f"FD API Call Forbidden (403) for {endpoint}. "
                "Check your plan limits."
            )
            fd_rotator.penalize(key)
            raise Exception(f"Forbidden (403) on key {key[:8]} for {endpoint}")
        if r.status_code == 429:
            logging.warning(f"FD API Call Rate-Limited (429) for {endpoint}.")
            fd_rotator.penalize(key)
            raise Exception(f"Rate-limited (429) on key {key[:8]} for {endpoint}")

        r.raise_for_status()

        try:
            data = r.json()
        except requests.exceptions.JSONDecodeError:
            logging.warning(f"FD API {endpoint} returned {r.status_code} "
                            "but no valid JSON.")
            data = {}

        fd_rotator.release(key)
        return data

    except Exception as e:
        logging.error(f"FD API call to {endpoint} failed: {e}")
        if key:
            fd_rotator.penalize(key)
        raise e


# ============ AS API (API-SPORTS) CALLER ============

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
            # Check if daily limit reset time has passed
            if time.time() > as_daily_reset_time:
                logging.info(
                    "API-Sports daily limit counter reset."
                )
                as_request_count = 0
                as_daily_reset_time = time.time() + (24 * 60 * 60)

            # Check if daily limit is exceeded
            if as_request_count >= 100:
                raise Exception(
                    "API-Sports 100 requests/day limit exceeded. "
                    "Try again tomorrow."
                )

            # Increment daily counter
            as_request_count += 1
            logging.info(
                f"API-Sports request {as_request_count}/100 for the day."
            )

        # Make the request (outside the lock)
        r = http_session.get(
            f"{AS_BASE_URL}{endpoint}",
            headers=AS_HEADERS,
            params=params,
            timeout=20,
        )

        # Respect 10/min limit by sleeping *after* every call
        time.sleep(6.5)  # 60s / 10 req = 6s. Add 0.5s buffer.

        r.raise_for_status()
        data = r.json()

        if not data.get("response") and data.get("errors"):
            logging.error(f"AS API Error: {data['errors']}")
            if isinstance(data['errors'], dict) and \
               'token' in data['errors']:
                raise Exception("API-Sports key is invalid or blocked.")
            return {} # Return empty for other errors (e.g., "bad parameters")

        return data

    except Exception as e:
        logging.error(f"AS API call to {endpoint} failed: {e}")
        raise e


# ============ SCHEMA CREATION ============
def create_tables(cur):
    """
    Creates all tables, including new columns for AS ID mapping.
    """
    logging.info("Verifying database schema...")
    create_statements = [
        # --- Existing Tables ---
        """
        CREATE TABLE IF NOT EXISTS areas (
            area_id INTEGER PRIMARY KEY,
            name VARCHAR(255) NOT NULL,
            code VARCHAR(10),
            flag VARCHAR(1024)
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS competitions (
            competition_id INTEGER PRIMARY KEY,
            area_id INTEGER REFERENCES areas(area_id),
            name VARCHAR(255) NOT NULL,
            code VARCHAR(50),
            type VARCHAR(50),
            emblem VARCHAR(1024),
            last_updated TIMESTAMP WITH TIME ZONE
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS teams (
            team_id INTEGER PRIMARY KEY,
            area_id INTEGER REFERENCES areas(area_id),
            name VARCHAR(255) NOT NULL,
            short_name VARCHAR(100),
            tla VARCHAR(10),
            crest VARCHAR(1024),
            address VARCHAR(512),
            website VARCHAR(512),
            founded INTEGER,
            club_colors VARCHAR(100),
            venue VARCHAR(255),
            last_updated TIMESTAMP WITH TIME ZONE
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS persons (
            person_id INTEGER PRIMARY KEY,
            name VARCHAR(255) NOT NULL,
            first_name VARCHAR(255),
            last_name VARCHAR(255),
            date_of_birth DATE,
            nationality VARCHAR(100),
            position VARCHAR(100),
            last_updated TIMESTAMP WITH TIME ZONE
        );
        """,
        # (All other existing tables: squads, matches, standings_lists,
        # standing_rows, scorers, match_team_details, lineups,
        # match_team_stats, goals, bookings, substitutions,
        # match_referees, match_odds, backfill_progress,
        # sync_state, predictions)
        # ... (Assuming they exist as per the provided v7.3) ...
        """
        CREATE TABLE IF NOT EXISTS matches (
            match_id INTEGER PRIMARY KEY,
            competition_id INTEGER REFERENCES competitions(competition_id),
            season_year INTEGER,
            utc_date TIMESTAMP WITH TIME ZONE,
            status VARCHAR(50),
            matchday INTEGER,
            stage VARCHAR(100),
            group_name VARCHAR(100),
            home_team_id INTEGER REFERENCES teams(team_id),
            away_team_id INTEGER REFERENCES teams(team_id),
            score_winner VARCHAR(50),
            score_duration VARCHAR(50),
            score_fulltime_home INTEGER,
            score_fulltime_away INTEGER,
            score_halftime_home INTEGER,
            score_halftime_away INTEGER,
            venue VARCHAR(255),
            attendance INTEGER,
            last_updated TIMESTAMP WITH TIME ZONE,
            raw_data JSONB,
            details_populated BOOLEAN DEFAULT FALSE
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS standings_lists (
            standings_list_id BIGSERIAL PRIMARY KEY,
            competition_id INTEGER REFERENCES competitions(competition_id),
            season_year INTEGER NOT NULL,
            stage VARCHAR(100),
            type VARCHAR(20) NOT NULL,
            group_name VARCHAR(100),
            UNIQUE (competition_id, season_year, type, stage, group_name)
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS standing_rows (
            standing_row_id BIGSERIAL PRIMARY KEY,
            standings_list_id BIGINT NOT NULL REFERENCES standings_lists(standings_list_id) ON DELETE CASCADE,
            team_id INTEGER NOT NULL REFERENCES teams(team_id),
            position INTEGER,
            played_games INTEGER,
            form VARCHAR(50),
            won INTEGER,
            draw INTEGER,
            lost INTEGER,
            points INTEGER,
            goals_for INTEGER,
            goals_against INTEGER,
            goal_difference INTEGER,
            UNIQUE (standings_list_id, team_id)
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS backfill_progress (
            competition_id INTEGER,
            season_year INTEGER,
            status VARCHAR(20) DEFAULT 'PENDING',
            last_updated TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
            task_type VARCHAR(10) DEFAULT 'FD',
            PRIMARY KEY (competition_id, season_year)
        );
        """,
        # --- NEW/ALTERED Tables for v8.0 ---
        """
        ALTER TABLE competitions
        ADD COLUMN IF NOT EXISTS as_competition_id INTEGER UNIQUE;
        """,
        """
        ALTER TABLE teams
        ADD COLUMN IF NOT EXISTS as_team_id INTEGER UNIQUE;
        """,
        """
        ALTER TABLE backfill_progress
        ADD COLUMN IF NOT EXISTS task_type VARCHAR(10) DEFAULT 'FD';
        """,
    ]

    # Execute create statements
    for statement in create_statements:
        try:
            cur.execute(statement)
        except Exception as e:
            # Ignore "relation already exists" or "column already exists"
            if "already exists" in str(e):
                pass
            else:
                logging.error(f"Schema update failed: {e}")
                raise e

    logging.info("All tables verified successfully.")


# ============ FD SMART UPSERTS (Unchanged) ============
# These functions are used by the FD populator and the AS transformers

def upsert_areas(cur, areas_data: List[Dict]):
    """Upserts a list of areas (FD format)."""
    if not areas_data:
        return
    sql = """
    INSERT INTO areas (area_id, name, code, flag)
    VALUES %s
    ON CONFLICT (area_id) DO UPDATE SET
        name = EXCLUDED.name,
        code = EXCLUDED.code,
        flag = EXCLUDED.flag;
    """
    unique_areas = {}
    for a in areas_data:
        if a and a.get('id'):
            # --- v8.1 FIX: Handle null names from AS ---
            if not a.get('name'):
                a['name'] = a.get('code') or 'Unknown Area'
            # --- End Fix ---
            unique_areas[a['id']] = a

    values = [
        (a["id"], a.get("name"), a.get("code"), a.get("flag"))
        for a in unique_areas.values()
    ]
    if values:
        execute_values(cur, sql, values)


def upsert_competitions(cur, comps_data: List[Dict]):
    """Upserts a list of competitions (FD format)."""
    if not comps_data:
        return
    sql = """
    INSERT INTO competitions (competition_id, area_id, name, code, type,
                              emblem, last_updated, as_competition_id)
    VALUES %s
    ON CONFLICT (competition_id) DO UPDATE SET
        area_id = EXCLUDED.area_id,
        name = EXCLUDED.name,
        code = EXCLUDED.code,
        type = EXCLUDED.type,
        emblem = EXCLUDED.emblem,
        last_updated = EXCLUDED.last_updated,
        as_competition_id = EXCLUDED.as_competition_id;
    """
    values = [
        (
            c["id"],
            c.get("area", {}).get("id"),
            c.get("name"),
            c.get("code"),
            c.get("type"),
            c.get("emblem"),
            c.get("lastUpdated"),
            c.get("as_competition_id")  # New mapped field
        )
        for c in comps_data if c.get('id')
    ]
    if values:
        upsert_areas(
            cur, [c.get("area", {}) for c in comps_data if c.get("area")]
        )
        execute_values(cur, sql, values)


def upsert_teams(cur, teams_data: List[Dict]):
    """Upserts a list of teams (FD format)."""
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
            t.get("as_team_id")  # New mapped field
        )
        for t in unique_teams.values()
    ]
    if values:
        execute_values(cur, sql, values, page_size=100)


def upsert_matches_basic(cur, competition_id: int, season_year: int,
                         matches_data: List[Dict]):
    """Upserts a list of basic match info (FD format)."""
    if not matches_data:
        return 0
    sql = """
    INSERT INTO matches (match_id, competition_id, season_year, utc_date,
                         status, matchday, stage, group_name, home_team_id,
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
        score = m.get("score", {})
        fullTime = score.get("fullTime", {})
        halfTime = score.get("halfTime", {})
        home_team = m.get("homeTeam", {})
        away_team = m.get("awayTeam", {})

        if home_team and home_team.get('id'):
            teams_to_upsert.append(home_team)
        if away_team and away_team.get('id'):
            teams_to_upsert.append(away_team)

        values.append((
            m["id"],
            competition_id,
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


def upsert_standings(cur, competition_id: int, season_year: int,
                     standings_data: List[Dict]):
    """Upserts a full standings response (FD format)."""
    if not standings_data:
        return
    for standing in standings_data:
        stage = standing.get("stage")
        st_type = standing.get("type")
        group = standing.get("group")

        cur.execute("""
            INSERT INTO standings_lists (competition_id, season_year,
                                         stage, type, group_name)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (competition_id, season_year, type, stage, group_name)
            DO UPDATE SET competition_id = EXCLUDED.competition_id
            RETURNING standings_list_id;
        """, (competition_id, season_year, stage, st_type, group))

        standings_list_id_tuple = cur.fetchone()
        if not standings_list_id_tuple:
            logging.warning(
                "Failed to get standings_list_id, skipping row insert."
            )
            continue
        standings_list_id = standings_list_id_tuple[0]

        table = standing.get("table", [])
        if not table:
            continue

        teams_to_upsert = [
            row.get("team") for row in table if row.get("team")
        ]
        upsert_teams(cur, teams_to_upsert)

        sql_rows = """
        INSERT INTO standing_rows (standings_list_id, team_id, position,
                                   played_games, form, won, draw, lost,
                                   points, goals_for, goals_against,
                                   goal_difference)
        VALUES %s
        ON CONFLICT (standings_list_id, team_id) DO UPDATE SET
            position = EXCLUDED.position,
            played_games = EXCLUDED.played_games,
            form = EXCLUDED.form,
            won = EXCLUDED.won,
            draw = EXCLUDED.draw,
            lost = EXCLUDED.lost,
            points = EXCLUDED.points,
            goals_for = EXCLUDED.goals_for,
            goals_against = EXCLUDED.goals_against,
            goal_difference = EXCLUDED.goal_difference;
        """
        row_values = [
            (
                standings_list_id,
                row.get("team", {}).get("id"),
                row.get("position"),
                row.get("playedGames"),
                row.get("form"),
                row.get("won"),
                row.get("draw"),
                row.get("lost"),
                row.get("points"),
                row.get("goalsFor"),
                row.get("goalsAgainst"),
                row.get("goalDifference")
            )
            for row in table if row.get("team", {}).get("id")
        ]
        if row_values:
            execute_values(cur, sql_rows, row_values, page_size=100)


def populate_all_areas_once(cur):
    """Fetches and upserts all areas from FD /areas endpoint."""
    logging.info("Populating all areas from FD /areas endpoint...")
    try:
        areas_data = api_call_fd("areas").get("areas", [])
        upsert_areas(cur, areas_data)
        logging.info(f"Upserted {len(areas_data)} areas from FD.")
    except Exception as e:
        logging.error(f"Failed to populate all areas: {e}")
        raise e


# ============ AS (API-SPORTS) DATA TRANSFORMERS ============

def get_or_create_as_area(cur, as_country: Dict[str, Any]) -> int:
    """Finds an existing area (country) by name or creates it."""
    # --- START v8.1 FIX ---
    if not as_country:
        return None # Cannot proceed

    country_name = as_country.get("name")
    if not country_name:
        country_name = as_country.get("code") # Try code as fallback
    if not country_name:
        country_name = "Unknown Area" # Final fallback
    # --- END v8.1 FIX ---
    
    # 1. Try to find by name
    cur.execute("SELECT area_id FROM areas WHERE name = %s", (country_name,))
    result = cur.fetchone()
    if result:
        return result[0]
    
    # 2. Not found, create it
    try:
        # Find max ID and add 1.
        cur.execute("SELECT MAX(area_id) FROM areas")
        max_id = cur.fetchone()[0]
        # Start at a high number to avoid collisions with FD
        new_area_id = (max_id or 2000) + 1 

        area_data = {
            "id": new_area_id,
            "name": country_name,
            "code": as_country.get("code"),
            "flag": as_country.get("flag")
        }
        upsert_areas(cur, [area_data])
        logging.info(f"Created new area for {country_name} with ID {new_area_id}")
        return new_area_id
    except Exception as e:
        logging.warning(f"Failed to create new area {country_name}: {e}")
        return None


def transform_as_teams(as_teams_data: List[Dict]) -> List[Dict]:
    """Transforms a list of AS teams into FD format."""
    transformed_teams = []
    for item in as_teams_data:
        team = item.get("team", {})
        venue = item.get("venue", {})
        if not team.get("id"):
            continue

        transformed = {
            "id": team["id"] + AS_ID_OFFSET,
            "as_team_id": team["id"],
            "name": team.get("name"),
            "shortName": team.get("name"),
            "tla": team.get("code"),
            "crest": team.get("logo"),
            "address": venue.get("address"),
            "website": None,
            "founded": team.get("founded"),
            "clubColors": None,
            "venue": venue.get("name"),
            "lastUpdated": datetime.datetime.now(pytz.UTC).isoformat()
            # Area must be linked separately
        }
        transformed_teams.append(transformed)
    return transformed_teams


def transform_as_matches(as_fixtures_data: List[Dict],
                         competition_id_offset: int,
                         season_year: int) -> List[Dict]:
    """Transforms a list of AS fixtures into FD format."""
    transformed_matches = []
    for m in as_fixtures_data:
        fixture = m.get("fixture", {})
        teams = m.get("teams", {})
        goals = m.get("goals", {})
        score = m.get("score", {})

        if not fixture.get("id"):
            continue

        # Map AS status to FD status
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

        matchday_str = m.get("league", {}).get("round", "0")
        matchday = None
        try:
            # Extract number, e.g., "Regular Season - 3" -> 3
            matchday_num = re.findall(r'\d+', matchday_str)
            if matchday_num:
                matchday = int(matchday_num[-1])
        except Exception:
            pass # Keep matchday as None

        transformed = {
            "id": fixture["id"] + AS_ID_OFFSET,
            "competition_id": competition_id_offset,
            "season_year": season_year,
            "utcDate": fixture.get("date"),
            "status": status,
            "matchday": matchday,
            "stage": None,  # AS separates this
            "group": m.get("league", {}).get("group"),
            "homeTeam": {
                "id": teams.get("home", {}).get("id", 0) + AS_ID_OFFSET,
                "name": teams.get("home", {}).get("name")
            },
            "awayTeam": {
                "id": teams.get("away", {}).get("id", 0) + AS_ID_OFFSET,
                "name": teams.get("away", {}).get("name")
            },
            "score": {
                "winner": winner,
                "duration": "REGULAR",  # Default
                "fullTime": {"home": goals.get("home"), "away": goals.get("away")},
                "halfTime": {"home": score.get("halftime", {}).get("home"),
                             "away": score.get("halftime", {}).get("away")}
            },
            "lastUpdated": datetime.datetime.fromtimestamp(
                fixture.get("timestamp", 0), pytz.UTC
            ).isoformat(),
            "raw_data": m  # Store original AS data
        }
        transformed_matches.append(transformed)
    return transformed_matches


def transform_as_standings(as_standings_resp: List[Dict],
                           competition_id_offset: int,
                           season_year: int) -> List[Dict]:
    """Transforms a list of AS standings into FD format."""
    transformed_standings = []
    if not as_standings_resp:
        return []
    
    # AS wraps standings in a list, often of size 1
    for league_standing in as_standings_resp:
        standings_list = league_standing.get("league", {}).get("standings", [])
        
        for group_standing in standings_list:
            # group_standing is a list of team rows
            transformed_table = []
            for row in group_standing:
                team = row.get("team", {})
                transformed_table.append({
                    "position": row.get("rank"),
                    "team": {
                        "id": team.get("id", 0) + AS_ID_OFFSET,
                        "name": team.get("name"),
                        "crest": team.get("logo")
                    },
                    "playedGames": row.get("all", {}).get("played"),
                    "form": row.get("form"),
                    "won": row.get("all", {}).get("win"),
                    "draw": row.get("all", {}).get("draw"),
                    "lost": row.get("all", {}).get("lose"),
                    "points": row.get("points"),
                    "goalsFor": row.get("all", {}).get("goals", {}).get("for"),
                    "goalsAgainst": row.get("all", {}).get("goals", {}).get("against"),
                    "goalDifference": row.get("goalsDiff")
                })

            transformed_standings.append({
                "stage": "REGULAR_SEASON",  # AS default
                "type": "TOTAL",  # AS default
                "group": group_standing[0].get("group") if group_standing else None,
                "table": transformed_table
            })
    return transformed_standings


# ============ CONCURRENT BACKFILL LOGIC (HYBRID) ============

# Task Tuple is now:
# (competition_id, season_year, comp_name, is_current, task_type)
# task_type = 'FD' or 'AS'

def discover_fd_tasks() -> List[Tuple[int, int, str, bool, str]]:
    """
    Fetches all FD competitions and their seasons to build a task list.
    Upserts competitions, areas, and links AS IDs.
    """
    tasks = []
    logging.info("Fetching all FD competitions to build task list...")
    conn = None
    try:
        conn = db_pool.getconn()
        with conn.cursor() as cur:
            comps_data_raw = api_call_fd("competitions").get("competitions", [])
            
            # Filter for free codes and inject AS ID
            comps_data_to_upsert = []
            fd_comps_by_id = {}
            for comp in comps_data_raw:
                comp_code = comp.get("code")
                if comp_code in FREE_CODES:
                    comp["as_competition_id"] = FD_AS_LEAGUE_MAP.get(comp_code)
                    comps_data_to_upsert.append(comp)
                    fd_comps_by_id[comp['id']] = comp
            
            logging.info(
                f"{len(comps_data_to_upsert)} free FD competitions detected. "
                "Upserting with AS ID links..."
            )
            upsert_competitions(cur, comps_data_to_upsert)
            conn.commit()

            # Discover seasons for these competitions
            for idx, comp in enumerate(comps_data_to_upsert, 1):
                comp_id = comp["id"]
                comp_name = comp["name"]
                comp_code = comp.get("code", "")
                
                logging.info(
                    f"[{idx}/{len(comps_data_to_upsert)}] FD: Getting seasons "
                    f"for {comp_name} ({comp_id}) ({comp_code})"
                )
                try:
                    detail = api_call_fd(f"competitions/{comp_id}")
                    if not detail or not detail.get("seasons"):
                        logging.warning(f"No FD season data for {comp_name}")
                        continue

                    seasons = detail.get("seasons", [])
                    current_year_str = detail.get(
                        "currentSeason", {}
                    ).get("startDate", "")[:4]
                    current_year = int(
                        current_year_str
                    ) if current_year_str else None

                    for year in range(FD_START_YEAR, FD_END_YEAR + 1):
                        if any(
                            s and s.get("startDate") and
                            s["startDate"].startswith(str(year))
                            for s in seasons
                        ):
                            is_current = (year == current_year)
                            tasks.append(
                                (comp_id, year, comp_name, is_current, 'FD')
                            )
                except Exception as e:
                    logging.error(
                        f"Failed to get FD seasons for {comp_name}: {e}"
                    )

        logging.info(f"TOTAL FD TASKS DISCOVERED: {len(tasks)}")
        return tasks
    except Exception as e:
        logging.error(f"CRITICAL: Failed to get FD competitions list. {e}")
        if conn: conn.rollback()
        return []
    finally:
        if conn: db_pool.putconn(conn)


def discover_as_tasks() -> List[Tuple[int, int, str, bool, str]]:
    """
    Fetches all AS competitions for requested seasons to build a task list.
    """
    if not AS_API_KEY:
        logging.warning("Skipping AS task discovery: No API key provided.")
        return []
    
    tasks = []
    logging.info("Fetching all AS competitions to build task list...")
    conn = None
    try:
        conn = db_pool.getconn()
        with conn.cursor() as cur:
            # Get the AS IDs of the FD leagues to skip them
            fd_as_ids_to_skip = set(FD_AS_LEAGUE_MAP.values())
            logging.info(f"Will skip {len(fd_as_ids_to_skip)} AS leagues "
                         "that are covered by FD.")

            for season in AS_SEASONS:
                logging.info(f"AS: Discovering leagues for season {season}...")
                try:
                    leagues_resp = api_call_as(
                        "/leagues", {"season": season}
                    )
                    if not leagues_resp.get("response"):
                        logging.warning(f"No AS leagues found for {season}.")
                        continue
                    
                    comps_to_upsert = []
                    for item in leagues_resp.get("response", []):
                        as_league = item.get("league", {})
                        as_country = item.get("country", {})
                        if not as_league.get("id") or \
                           as_league["id"] in fd_as_ids_to_skip:
                            continue # Skip FD-covered leagues
                        
                        area_id = get_or_create_as_area(cur, as_country)
                        # v8.1: If area creation failed, we can't add league
                        if area_id is None:
                            logging.warning(
                                f"Skipping league {as_league.get('name')} "
                                "due to failed area creation."
                            )
                            continue
                        
                        comp_id_offset = as_league["id"] + AS_ID_OFFSET
                        comp_data = {
                            "id": comp_id_offset,
                            "area": {"id": area_id},
                            "name": as_league["name"],
                            "code": f"AS_{as_league['id']}", # Create a unique code
                            "type": as_league["type"],
                            "emblem": as_league["logo"],
                            "lastUpdated": datetime.datetime.now(
                                pytz.UTC
                            ).isoformat(),
                            "as_competition_id": as_league["id"]
                        }
                        comps_to_upsert.append(comp_data)
                        
                        # Add task
                        tasks.append(
                            (comp_id_offset, season, as_league["name"],
                             season == 2025, 'AS') # Assume 2025 is current
                        )
                    
                    # Upsert all discovered AS leagues for this season
                    upsert_competitions(cur, comps_to_upsert)
                    conn.commit()
                    logging.info(
                        f"Discovered and upserted {len(comps_to_upsert)} "
                        f"new AS leagues for {season}."
                    )
                
                except Exception as e:
                    logging.error(f"Failed to process AS season {season}: {e}")
                    if "limit" in str(e):
                        logging.CRITICAL("AS daily limit hit during discovery.")
                        break # Stop discovery for today
                    conn.rollback()

        logging.info(f"TOTAL AS TASKS DISCOVERED: {len(tasks)}")
        return tasks
    except Exception as e:
        logging.error(f"CRITICAL: Failed to get AS competitions list. {e}")
        if conn: conn.rollback()
        return []
    finally:
        if conn: db_pool.putconn(conn)


def get_pending_tasks(
    conn, all_discovered_tasks: List[Tuple[int, int, str, bool, str]]
) -> List[Tuple[int, int, str, bool, str]]:
    """
    Cross-references discovered tasks with the backfill_progress table.
    Returns only tasks that are NOT marked 'COMPLETED' or 'FAILED'.
    """
    if not all_discovered_tasks:
        return []

    task_keys = [(t[0], t[1], t[4]) for t in all_discovered_tasks]
    
    sql_insert = """
    INSERT INTO backfill_progress (competition_id, season_year, task_type)
    VALUES %s
    ON CONFLICT (competition_id, season_year) DO UPDATE
    SET task_type = EXCLUDED.task_type;
    """
    with conn.cursor() as cur:
        execute_values(cur, sql_insert, task_keys, page_size=1000)
        conn.commit()

        cur.execute(
            "SELECT competition_id, season_year FROM backfill_progress "
            "WHERE status != 'COMPLETED' AND status != 'FAILED'"
        )
        pending_keys = set(cur.fetchall()) # Set of (comp_id, year) tuples

    # Filter the main task list
    pending_tasks = [
        t for t in all_discovered_tasks if (t[0], t[1]) in pending_keys
    ]
    
    logging.info(
        f"Found {len(pending_keys)} tasks not yet completed or failed."
    )
    return pending_tasks


def process_season_task_fd(task: Tuple) -> bool:
    """The main worker function for FD tasks."""
    comp_id, year, comp_name, is_current, _ = task
    task_id = f"FD: {comp_name} ({comp_id}) / {year}"
    conn = None
    
    try:
        conn = db_pool.getconn()
        with conn.cursor() as cur:
            cur.execute("""
                UPDATE backfill_progress SET status = 'PENDING',
                       last_updated = NOW()
                WHERE competition_id = %s AND season_year = %s;
            """, (comp_id, year))
            conn.commit()
            
            logging.info(f"STARTING: {task_id}")

            # 4a. Get Matches
            logging.info(f"[{task_id}] Fetching matches...")
            matches_resp = api_call_fd(
                f"competitions/{comp_id}/matches", {"season": year}
            )
            matches_data = matches_resp.get("matches", [])
            saved_matches = upsert_matches_basic(
                cur, comp_id, year, matches_data
            )
            logging.info(f"[{task_id}] Staged {saved_matches} basic matches.")

            # 4b. Get Standings (only for current)
            if is_current:
                logging.info(f"[{task_id}] Fetching standings...")
                standings_resp = api_call_fd(
                    f"competitions/{comp_id}/standings", {"season": year}
                )
                standings_data = standings_resp.get("standings", [])
                upsert_standings(cur, comp_id, year, standings_data)
                logging.info(
                    f"[{task_id}] Staged {len(standings_data)} standing lists."
                )
            else:
                logging.info(f"[{task_id}] Skipping standings (not current).")

            # 4c. Get Teams
            logging.info(f"[{task_id}] Fetching teams...")
            teams_data = api_call_fd(
                f"competitions/{comp_id}/teams", {"season": year}
            ).get("teams", [])
            upsert_teams(cur, teams_data)
            logging.info(f"[{task_id}] Staged {len(teams_data)} teams.")

            cur.execute("""
                UPDATE backfill_progress SET status = 'COMPLETED',
                       last_updated = NOW()
                WHERE competition_id = %s AND season_year = %s;
            """, (comp_id, year))
            
            conn.commit()
            logging.info(f"*** COMPLETE: {task_id} ***")
            return True

    except Exception as e:
        if conn: conn.rollback()
        if "Forbidden (403)" in str(e):
            logging.CRITICAL(
                f"FAILED (403): {task_id}. This task is FORBIDDEN. "
                "Marking as 'FAILED' to prevent retry loop."
            )
            try:
                conn = db_pool.getconn()
                with conn.cursor() as cur:
                    cur.execute("""
                        UPDATE backfill_progress SET status = 'FAILED',
                               last_updated = NOW()
                        WHERE competition_id = %s AND season_year = %s;
                    """, (comp_id, year))
                    conn.commit()
            except Exception as db_e:
                logging.error(f"Failed to mark task {task_id} as FAILED: {db_e}")
        else:
            logging.error(f"FAILED: {task_id}. Error: {e}. Rolling back.")
        return False
    finally:
        if conn: db_pool.putconn(conn)


def process_season_task_as(task: Tuple) -> bool:
    """The main worker function for AS tasks."""
    comp_id_offset, year, comp_name, is_current, _ = task
    as_comp_id = comp_id_offset - AS_ID_OFFSET
    task_id = f"AS: {comp_name} ({as_comp_id}) / {year}"
    conn = None
    
    try:
        conn = db_pool.getconn()
        with conn.cursor() as cur:
            cur.execute("""
                UPDATE backfill_progress SET status = 'PENDING',
                       last_updated = NOW()
                WHERE competition_id = %s AND season_year = %s;
            """, (comp_id_offset, year))
            conn.commit()
            
            logging.info(f"STARTING: {task_id}")
            
            # 1. Get Teams (for this league/season)
            logging.info(f"[{task_id}] Fetching teams...")
            teams_resp = api_call_as(
                "/teams", {"league": as_comp_id, "season": year}
            )
            as_teams_data = teams_resp.get("response", [])
            transformed_teams = transform_as_teams(as_teams_data)
            upsert_teams(cur, transformed_teams)
            logging.info(f"[{task_id}] Staged {len(transformed_teams)} teams.")

            # 2. Get Matches
            logging.info(f"[{task_id}] Fetching matches...")
            matches_resp = api_call_as(
                "/fixtures", {"league": as_comp_id, "season": year}
            )
            as_matches_data = matches_resp.get("response", [])
            transformed_matches = transform_as_matches(
                as_matches_data, comp_id_offset, year
            )
            saved_matches = upsert_matches_basic(
                cur, comp_id_offset, year, transformed_matches
            )
            logging.info(f"[{task_id}] Staged {saved_matches} basic matches.")

            # 3. Get Standings
            # AS often allows historical standings
            logging.info(f"[{task_id}] Fetching standings...")
            standings_resp = api_call_as(
                "/standings", {"league": as_comp_id, "season": year}
            )
            as_standings_data = standings_resp.get("response", [])
            transformed_standings = transform_as_standings(
                as_standings_data, comp_id_offset, year
            )
            upsert_standings(cur, comp_id_offset, year, transformed_standings)
            logging.info(
                f"[{task_id}] Staged {len(transformed_standings)} standing lists."
            )

            cur.execute("""
                UPDATE backfill_progress SET status = 'COMPLETED',
                       last_updated = NOW()
                WHERE competition_id = %s AND season_year = %s;
            """, (comp_id_offset, year))
            
            conn.commit()
            logging.info(f"*** COMPLETE: {task_id} ***")
            return True

    except Exception as e:
        if conn: conn.rollback()
        # Check if it's a daily limit error
        if "limit" in str(e):
            logging.CRITICAL(
                f"FAILED (Limit): {task_id}. AS Daily limit hit. "
                "Task will be retried later."
            )
            # Don't mark as 'FAILED', just let it retry
        else:
             logging.error(f"FAILED: {task_id}. Error: {e}. Rolling back.")
        return False
    finally:
        if conn: db_pool.putconn(conn)


def process_task_wrapper(task: Tuple) -> bool:
    """Selects the correct worker function based on task type."""
    task_type = task[4]
    if task_type == 'AS':
        return process_season_task_as(task)
    elif task_type == 'FD':
        return process_season_task_fd(task)
    else:
        logging.error(f"Unknown task type: {task_type}")
        return False


# ============ MAIN ============
def main():
    start_time = time.time()
    try:
        conn = db_pool.getconn()
        with conn.cursor() as cur:
            create_tables(cur)
            populate_all_areas_once(cur) # Pre-populate FD areas
            conn.commit()
        db_pool.putconn(conn)
        
        wat = (
            pytz.timezone("Africa/Lagos")
            .localize(datetime.datetime.now())
            .strftime("%Y-%m-%d %I:%M %p WAT")
        )
        logging.info(f"POPULATOR {VERSION} (HYBRID) | {wat} | SMART COOLDOWN")
        logging.warning("Script will run until all tasks are 'COMPLETED' "
                        "or 'FAILED'.")
        logging.warning("AS tasks will pause if 100/day limit is hit.")

        # 2. Get all work from both sources
        all_discovered_tasks = discover_fd_tasks()
        all_discovered_tasks.extend(discover_as_tasks())
        
        if not all_discovered_tasks:
            logging.error("No tasks discovered from any source. Exiting.")
            return

        # 3. Indefinite Retry Loop
        while True:
            conn = db_pool.getconn()
            tasks_to_run = get_pending_tasks(conn, all_discovered_tasks)
            db_pool.putconn(conn)

            if not tasks_to_run:
                logging.info(
                    "--- ALL TASKS COMPLETE! (or marked as FAILED) ---"
                )
                break  # Exit the while loop

            logging.info(
                f"--- STARTING RUN: {len(tasks_to_run)} tasks to process ---"
            )
            
            # Prioritize FD tasks
            tasks_to_run.sort(key=lambda x: x[4] == 'AS') # Puts 'AS' last

            with ThreadPoolExecutor(
                max_workers=MAX_WORKERS, thread_name_prefix="Worker"
            ) as executor:
                # Use the wrapper to process tasks
                results = list(executor.map(process_task_wrapper, tasks_to_run))

            failed_tasks_count = sum(1 for res in results if not res)
            
            if failed_tasks_count == 0:
                logging.info(
                    "--- RUN FINISHED: All tasks in this batch succeeded. ---"
                )
            else:
                logging.warning(
                    f"--- RUN FAILED: {failed_tasks_count} tasks failed. "
                    f"Retrying non-403/non-limit errors in "
                    f"{RETRY_SLEEP_SECONDS} seconds... ---"
                )
                time.sleep(RETRY_SLEEP_SECONDS)
            
            # Check for AS limit just in case
            if as_request_count >= 100:
                logging.CRITICAL(
                    "AS Daily Limit Hit. Pausing populator for 1 hour."
                )
                time.sleep(3600)

    except Exception as e:
        logging.error(f"Main process failed: {e}")
    finally:
        db_pool.closeall()
        end_time = time.time()
        logging.info(
            "Database connection pool closed. "
            f"Total runtime: {end_time - start_time:.2f} seconds."
        )


if __name__ == "__main__":
    main()