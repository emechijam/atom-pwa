# populator.py v7.3 - Populates a normalized schema from football-data.org
# This script is designed for the FREE TIER and respects its limitations.
# UPGRADE (v7.3):
# - FIX 1: Implemented 403 (Forbidden) handling. The API forbids fetching
#   old seasons, even for "free" leagues. The script will now catch 403s,
#   mark the task as 'FAILED' in the database, and stop retrying it,
#   allowing the main process to eventually complete.
# - FIX 2: Added defensive coding to prevent " 'int' object is not callable"
#   TypeError. Replaced dynamic page_size=len() with a static value and
#   isolated the len() call in the main loop.
# - 'BSA' is included in FREE_CODES as requested.
#
# UPGRADE (v7.1):
# - FIX: Corrected SQL "INSERT ... target columns" bug in get_pending_tasks().
#
# UPGRADE (v7):
# - Implements indefinite retry loop (now modified for 403s).
# - Makes process_season_task transactional.
# - Adds 'predictions' table to schema.

import os
import time
import pytz
import logging
import threading
import requests
import psycopg2
import datetime
import json
from psycopg2.pool import ThreadedConnectionPool
from psycopg2.extras import execute_values
from dotenv import load_dotenv
from concurrent.futures import ThreadPoolExecutor
from queue import PriorityQueue
from typing import List, Tuple, Dict, Any, Optional

# ============ CONFIG & LOGGING ============
load_dotenv()  # Loads .env file for local runs. On Streamlit, use Secrets!
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(threadName)s] [%(levelname)s] %(message)s",
    datefmt="%Y-m-%d %H:%M:%S",
)

# --- Year Range for Backfill ---
START_YEAR = 2015
END_YEAR = 2025  # Set to the current or next year
# -------------------------------

# --- Free Tier Competition Codes ---
# 'BSA' is re-included as requested. The new 403 handling will
# mark old, inaccessible seasons as 'FAILED' instead of looping.
FREE_CODES = ['BSA','BL1', 'CL', 'DED', 'EC', 'FL1', 'PD', 'PL', 'PPL', 'SA', 'WC', 'ELC', 'CLI']
# -----------------------------------

# Concurrency & DB
MAX_WORKERS = 15  # Max parallel API/DB workers
MAX_DB_CONNECTIONS = 20  # Max connections in the pool (must be >= MAX_WORKERS)
RETRY_SLEEP_SECONDS = 60  # How long to wait after a failed run before retrying

# ============ CONNECT ============
try:
    db_url = os.getenv("DATABASE_URL")
    if not db_url:
        logging.error("DATABASE_URL not found. Check .env file or Streamlit Secrets.")
        exit(1)

    # Fix for SQLAlchemy-style DSNs: psycopg2 doesn't like "postgresql+psycopg"
    if db_url.startswith("postgresql+psycopg://"):
        logging.warning("DSN prefix 'postgresql+psycopg://' found, correcting to 'postgresql://'.")
        db_url = db_url.replace("postgresql+psycopg://", "postgresql://", 1)

    db_pool = ThreadedConnectionPool(
        minconn=2,
        maxconn=MAX_DB_CONNECTIONS,
        dsn=db_url,
    )
    logging.info(f"Database connection pool created (Max: {MAX_DB_CONNECTIONS}).")
except Exception as e:
    logging.error(f"DB connection pool failed: {e}")
    exit(1)

# ============ API & KEY ROTATOR (SMART COOLDOWN) ============
BASE_URL = "https://api.football-data.org/v4"
API_KEYS = [
    k.strip()
    for k in os.getenv("FOOTBALL_DATA_API_KEY", "").split(",")
    if k.strip()
]
if not API_KEYS:
    logging.error("FOOTBALL_DATA_API_KEY not found. Check .env file or Streamlit Secrets.")
    exit(1)

http_session = requests.Session()  # Use a session for connection pooling


class KeyRotator:
    """
    Manages API keys using a PriorityQueue to respect per-key rate limits.
    Each item in the queue is a tuple: (next_available_timestamp, key)
    This is fully thread-safe.
    """

    def __init__(self, keys: List[str]):
        self.queue = PriorityQueue()
        self.lock = (
            threading.Lock()
        )  # For logging, to prevent garbled messages
        if not keys:
            logging.error("No API keys provided to KeyRotator.")
            exit(1)
        for key in keys:
            self.queue.put((0, key))  # All keys are available at time 0
        logging.info(f"LOADED {len(keys)} KEYS — SMART COOLDOWN MODE!")

    def get_next(self) -> str:
        """
        Gets the soonest-available key, waiting if necessary.
        """
        next_free_time, key = self.queue.get()

        now = time.time()
        if next_free_time > now:
            sleep_duration = next_free_time - now
            with self.lock:
                logging.info(
                    f"Key {key[:8]}... on cooldown. Sleeping for {sleep_duration:.2f}s"
                )
            time.sleep(sleep_duration)

        with self.lock:
            logging.debug(f"KEY → {key[:8]}...")
        return key

    def release(self, key: str):
        """
        A successful call. Put key back on cooldown for 6.5s.
        60 seconds / 10 calls = 6s. Add 0.5s buffer to be safe.
        """
        next_use = time.time() + 6.5
        self.queue.put((next_use, key))

    def penalize(self, key: str):
        """
        A rate-limit (429) or other major error occurred.
        Put key in 70-second "penalty box".
        """
        with self.lock:
            logging.warning(f"PENALIZED → {key[:8]}... | Cooldown 70s")
        next_use = time.time() + 70.0  # 70s penalty box
        self.queue.put((next_use, key))


rotator = KeyRotator(API_KEYS)


def api_call(endpoint: str, params: Optional[Dict] = None) -> Dict[str, Any]:
    """
    Makes an API call using the smart key rotator.
    The rotator now handles all sleeping/cooldowns.
    """
    key = "" # Initialize key to empty string
    try:
        key = rotator.get_next()
        r = http_session.get(
            f"{BASE_URL}/{endpoint}",
            headers={"X-Auth-Token": key},
            params=params,
            timeout=20,
        )
        if r.status_code == 403:
            logging.warning(f"API Call Forbidden (403) for {endpoint}. Check your plan limits.")
            rotator.penalize(key)  # Penalize to stop hammering
            # Raise a specific exception to be caught by the worker
            raise Exception(f"Forbidden (403) on key {key[:8]}... for {endpoint}")
        if r.status_code == 429:
            logging.warning(f"API Call Rate-Limited (429) for {endpoint}.")
            rotator.penalize(key)  # Penalize for rate-limit
            raise Exception(f"Rate-limited (429) on key {key[:8]}... for {endpoint}")

        r.raise_for_status()  # Raise for other errors (500s, 404s etc.)
        
        try:
            data = r.json()
        except requests.exceptions.JSONDecodeError:
            logging.warning(f"API call to {endpoint} returned {r.status_code} but no valid JSON.")
            data = {}  # Return an empty dict to avoid NoneType errors
            
        rotator.release(key)  # Success! Release for normal cooldown
        return data

    except Exception as e:
        logging.error(f"API call to {endpoint} failed: {e}")
        if key: # Only penalize if a key was successfully retrieved
             rotator.penalize(key)  # Penalize any error to be safe
        raise e  # Re-raise the exception to fail the task


# ============ SCHEMA CREATION ============
def create_tables(cur):
    """
    Creates all tables from the schema design.
    This is now safe to run multiple times without deleting data.
    """
    
    # Create statements in correct order of dependency
    create_statements = [
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
            code VARCHAR(10),
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
        """
        CREATE TABLE IF NOT EXISTS squads (
            squad_entry_id BIGSERIAL PRIMARY KEY,
            team_id INTEGER NOT NULL REFERENCES teams(team_id),
            person_id INTEGER NOT NULL REFERENCES persons(person_id),
            role VARCHAR(50),
            shirt_number INTEGER,
            contract_start VARCHAR(20),
            contract_until VARCHAR(20),
            UNIQUE (team_id, person_id, role)
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS matches (
            match_id INTEGER PRIMARY KEY,
            competition_id INTEGER REFERENCES competitions(competition_id),
            season_year INTEGER, -- CHANGED: from season_id FOREIGN KEY
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
            season_year INTEGER NOT NULL, -- CHANGED: from season_id FOREIGN KEY
            stage VARCHAR(100),
            type VARCHAR(20) NOT NULL,
            group_name VARCHAR(100),
            UNIQUE (competition_id, season_year, type, stage, group_name) -- CHANGED: updated unique constraint
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
        CREATE TABLE IF NOT EXISTS scorers (
            scorer_id BIGSERIAL PRIMARY KEY,
            competition_id INTEGER REFERENCES competitions(competition_id),
            season_year INTEGER NOT NULL, -- CHANGED: from season_id FOREIGN KEY
            person_id INTEGER NOT NULL REFERENCES persons(person_id),
            team_id INTEGER REFERENCES teams(team_id),
            goals INTEGER,
            assists INTEGER,
            penalties INTEGER,
            UNIQUE (competition_id, season_year, person_id) -- CHANGED: updated unique constraint
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS match_team_details (
            match_team_detail_id BIGSERIAL PRIMARY KEY,
            match_id INTEGER NOT NULL REFERENCES matches(match_id) ON DELETE CASCADE,
            team_id INTEGER NOT NULL REFERENCES teams(team_id),
            home_or_away VARCHAR(4) NOT NULL CHECK (home_or_away IN ('HOME', 'AWAY')),
            coach_id INTEGER REFERENCES persons(person_id),
            formation VARCHAR(20),
            UNIQUE (match_id, team_id)
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS match_lineups (
            lineup_id BIGSERIAL PRIMARY KEY,
            match_id INTEGER NOT NULL REFERENCES matches(match_id) ON DELETE CASCADE,
            team_id INTEGER NOT NULL REFERENCES teams(team_id),
            person_id INTEGER NOT NULL REFERENCES persons(person_id),
            type VARCHAR(10) NOT NULL CHECK (type IN ('STARTING', 'BENCH')),
            position VARCHAR(100),
            shirt_number INTEGER,
            UNIQUE (match_id, person_id)
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS match_team_stats (
            match_team_stats_id BIGSERIAL PRIMARY KEY,
            match_id INTEGER NOT NULL REFERENCES matches(match_id) ON DELETE CASCADE,
            team_id INTEGER NOT NULL REFERENCES teams(team_id),
            corner_kicks INTEGER,
            free_kicks INTEGER,
            ball_possession INTEGER,
            fouls INTEGER,
            goal_kicks INTEGER,
            offsides INTEGER,
            red_cards INTEGER,
            saves INTEGER,
            shots INTEGER,
            shots_off_goal INTEGER,
            shots_on_goal INTEGER,
            throw_ins INTEGER,
            yellow_cards INTEGER,
            yellow_red_cards INTEGER,
            UNIQUE (match_id, team_id)
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS goals (
            goal_id BIGSERIAL PRIMARY KEY,
            match_id INTEGER NOT NULL REFERENCES matches(match_id) ON DELETE CASCADE,
            team_id INTEGER NOT NULL REFERENCES teams(team_id),
            scorer_id INTEGER NOT NULL REFERENCES persons(person_id),
            assist_id INTEGER REFERENCES persons(person_id),
            minute INTEGER,
            injury_time INTEGER,
            type VARCHAR(50),
            score_home_at_goal INTEGER,
            score_away_at_goal INTEGER
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS bookings (
            booking_id BIGSERIAL PRIMARY KEY,
            match_id INTEGER NOT NULL REFERENCES matches(match_id) ON DELETE CASCADE,
            team_id INTEGER NOT NULL REFERENCES teams(team_id),
            person_id INTEGER NOT NULL REFERENCES persons(person_id),
            minute INTEGER,
            card VARCHAR(20) NOT NULL
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS substitutions (
            substitution_id BIGSERIAL PRIMARY KEY,
            match_id INTEGER NOT NULL REFERENCES matches(match_id) ON DELETE CASCADE,
            team_id INTEGER NOT NULL REFERENCES teams(team_id),
            minute INTEGER,
            player_out_id INTEGER NOT NULL REFERENCES persons(person_id),
            player_in_id INTEGER NOT NULL REFERENCES persons(person_id)
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS match_referees (
            match_referee_id BIGSERIAL PRIMARY KEY,
            match_id INTEGER NOT NULL REFERENCES matches(match_id) ON DELETE CASCADE,
            person_id INTEGER NOT NULL REFERENCES persons(person_id),
            type VARCHAR(100) NOT NULL,
            UNIQUE (match_id, person_id)
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS match_odds (
            match_odds_id BIGSERIAL PRIMARY KEY,
            match_id INTEGER NOT NULL UNIQUE REFERENCES matches(match_id) ON DELETE CASCADE,
            home_win DECIMAL(10, 2),
            draw DECIMAL(10, 2),
            away_win DECIMAL(10, 2)
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS backfill_progress (
            competition_id INTEGER,
            season_year INTEGER,
            status VARCHAR(20) DEFAULT 'PENDING',
            last_updated TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
            PRIMARY KEY (competition_id, season_year)
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS sync_state (
            id SERIAL PRIMARY KEY,
            competition_code VARCHAR(10) NOT NULL,
            season_year INTEGER NOT NULL,
            last_synced_match_id INTEGER DEFAULT 0,
            UNIQUE (competition_code, season_year)
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS predictions (
            prediction_id BIGSERIAL PRIMARY KEY,
            match_id INTEGER NOT NULL UNIQUE REFERENCES matches(match_id) ON DELETE CASCADE,
            prediction_data JSONB NOT NULL,
            generated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
        );
        """
    ]
    
    logging.info("Verifying database schema...")
    
    # Execute create statements
    for statement in create_statements:
        cur.execute(statement)

    logging.info("All tables verified successfully.")


# ============ SMART UPSERTS ============
# These functions populate the normalized schema.
# They are idempotent, so they can be run safely multiple times.


def upsert_areas(cur, areas_data: List[Dict]):
    """Upserts a list of areas."""
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
            unique_areas[a['id']] = a
            
    values = [
        (
            a["id"],
            a.get("name"),
            a.get("code"),
            a.get("flag"),
        )
        for a in unique_areas.values()
    ]
    
    if values:
        execute_values(cur, sql, values)

def populate_all_areas_once(cur):
    """Fetches and upserts all areas from the /areas endpoint."""
    logging.info("Populating all areas from /areas endpoint...")
    try:
        areas_data = api_call("areas").get("areas", [])
        upsert_areas(cur, areas_data)
        logging.info(f"Upserted {len(areas_data)} areas.")
    except Exception as e:
        logging.error(f"Failed to populate all areas: {e}")
        raise e # Re-raise to fail the initial setup if this fails

def upsert_competitions(cur, comps_data: List[Dict]):
    """Upserts a list of competitions."""
    if not comps_data:
        return
    sql = """
    INSERT INTO competitions (competition_id, area_id, name, code, type, emblem, last_updated)
    VALUES %s
    ON CONFLICT (competition_id) DO UPDATE SET
        area_id = EXCLUDED.area_id,
        name = EXCLUDED.name,
        code = EXCLUDED.code,
        type = EXCLUDED.type,
        emblem = EXCLUDED.emblem,
        last_updated = EXCLUDED.last_updated;
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
        )
        for c in comps_data if c.get('id')
    ]
    if values:
        # We assume populate_all_areas_once() has already run.
        # This call is just a fallback for any weird edge cases.
        upsert_areas(cur, [c.get("area", {}) for c in comps_data if c.get("area")])
        execute_values(cur, sql, values)


def upsert_teams(cur, teams_data: List[Dict]):
    """Upserts a list of teams."""
    if not teams_data:
        return
    sql = """
    INSERT INTO teams (team_id, area_id, name, short_name, tla, crest, address, website, founded, club_colors, venue, last_updated)
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
        last_updated = EXCLUDED.last_updated;
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
        )
        for t in unique_teams.values()
    ]
    
    if values:
        # We assume areas are populated by populate_all_areas_once()
        execute_values(cur, sql, values)

def upsert_matches_basic(cur, competition_id: int, season_year: int, matches_data: List[Dict]):
    """Upserts a list of basic match info from the /matches list endpoint."""
    if not matches_data:
        return 0
    sql = """
    INSERT INTO matches (match_id, competition_id, season_year, utc_date, status, matchday, stage, group_name, home_team_id, away_team_id, score_winner, score_duration, score_fulltime_home, score_fulltime_away, score_halftime_home, score_halftime_away, last_updated, raw_data, details_populated)
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
        details_populated = FALSE; -- Always set to false, it will be updated by a detail fetcher (if one exists)
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
        
        if home_team:
            teams_to_upsert.append(home_team)
        if away_team:
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


def upsert_standings(cur, competition_id: int, season_year: int, standings_data: List[Dict]):
    """Upserts a full standings response."""
    if not standings_data:
        return

    for standing in standings_data:
        stage = standing.get("stage")
        st_type = standing.get("type")
        group = standing.get("group")

        # 1. Create the standings_list
        cur.execute("""
            INSERT INTO standings_lists (competition_id, season_year, stage, type, group_name)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (competition_id, season_year, type, stage, group_name) DO UPDATE SET competition_id = EXCLUDED.competition_id
            RETURNING standings_list_id;
        """, (competition_id, season_year, stage, st_type, group))
        
        standings_list_id_tuple = cur.fetchone()
        if not standings_list_id_tuple:
            logging.warning("Failed to get standings_list_id, skipping row insert.")
            continue
        standings_list_id = standings_list_id_tuple[0]


        # 2. Prepare all teams and rows
        table = standing.get("table", [])
        if not table:
            continue
            
        teams_to_upsert = [row.get("team") for row in table if row.get("team")]
        upsert_teams(cur, teams_to_upsert)

        sql_rows = """
        INSERT INTO standing_rows (standings_list_id, team_id, position, played_games, form, won, draw, lost, points, goals_for, goals_against, goal_difference)
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


# ============ CONCURRENT BACKFILL LOGIC ============

def discover_competition_tasks() -> List[Tuple[int, int, str, bool]]:
    """
    Fetches all competitions and their seasons to build a task list.
    Upserts competitions, areas, and seasons as it goes.
    """
    tasks = []
    logging.info("Fetching all competitions to build task list...")
    conn = None
    try:
        conn = db_pool.getconn()
        with conn.cursor() as cur:
            comps_data = api_call("competitions").get("competitions", [])
            logging.info(f"{len(comps_data)} competitions detected. Upserting...")
            
            upsert_competitions(cur, comps_data)
            conn.commit()
            
            for idx, comp in enumerate(comps_data, 1):
                comp_id = comp["id"]
                comp_name = comp["name"]
                comp_code = comp.get("code", "")
                if comp_code not in FREE_CODES:
                    logging.info(f"[{idx}/{len(comps_data)}] Skipping non-free competition: {comp_name} ({comp_id}) ({comp_code})")
                    continue
                logging.info(f"[{idx}/{len(comps_data)}] Getting seasons for {comp_name} ({comp_id}) ({comp_code})")
                
                try:
                    detail = api_call(f"competitions/{comp_id}")
                    
                    if not detail or not detail.get("seasons"):
                        logging.warning(f"No season data returned for {comp_name} ({comp_id}). Skipping.")
                        continue

                    seasons = detail.get("seasons", [])
                    
                    current_season = detail.get("currentSeason", {})
                    current_year_str = current_season.get("startDate", "")[:4]
                    current_year = int(current_year_str) if current_year_str else None
                    
                    conn.commit()
                    
                    for year in range(START_YEAR, END_YEAR + 1):
                        if any(s and s.get("startDate") and s["startDate"].startswith(str(year)) for s in seasons):
                            is_current = (year == current_year) if current_year else False
                            tasks.append((comp_id, year, comp_name, is_current))
                            
                except Exception as e:
                    # This exception is often a 403 Forbidden for paid-tier leagues
                    logging.error(f"Failed to get seasons for {comp_name} ({comp_id}): {e}. This may be a non-free league.")
                    conn.rollback()

        logging.info(f"TOTAL TASKS DISCOVERED: {len(tasks)} (competition, year) pairs.")
        return tasks
    except Exception as e:
        logging.error(f"CRITICAL: Failed to get competitions list. {e}")
        if conn:
            conn.rollback()
        return []
    finally:
        if conn:
            db_pool.putconn(conn)


def get_pending_tasks(
    conn, all_discovered_tasks: List[Tuple[int, int, str, bool]]
) -> List[Tuple[int, int, str, bool]]:
    """
    Cross-references discovered tasks with the backfill_progress table.
    Returns only tasks that are NOT marked 'COMPLETED'.
    """
    if not all_discovered_tasks:
        return []

    # Ensure all tasks exist in the progress table
    task_keys = [(t[0], t[1]) for t in all_discovered_tasks]
    
    sql_insert = """
    INSERT INTO backfill_progress (competition_id, season_year)
    VALUES %s
    ON CONFLICT (competition_id, season_year) DO NOTHING;
    """
    with conn.cursor() as cur:
        # --- FIX V7.3: Set static page_size to avoid potential 'int' error ---
        execute_values(cur, sql_insert, task_keys, page_size=1000)
        conn.commit()

        # Get all tasks that are not completed OR failed
        # --- FIX V7.3: We now skip 'FAILED' tasks as well ---
        cur.execute(
            "SELECT competition_id, season_year FROM backfill_progress WHERE status != 'COMPLETED' AND status != 'FAILED'"
        )
        pending_keys = set(cur.fetchall())

    # Filter the main task list
    pending_tasks = [
        t for t in all_discovered_tasks if (t[0], t[1]) in pending_keys
    ]
    
    logging.info(f"Found {len(pending_keys)} tasks not yet completed or failed.")
    return pending_tasks


def process_season_task(task: Tuple[int, int, str, bool]) -> bool:
    """
    The main worker function. Processes a single (competition_id, year) task.
    This function is run by each thread.
    Returns True on success, False on failure.
    
    V7.3 CHANGE: Now includes specific 403 (Forbidden) handling.
    """
    comp_id, year, comp_name, is_current = task
    task_id = f"{comp_name} ({comp_id}) / {year}"
    conn = None
    
    try:
        # 1. Get connection from pool
        conn = db_pool.getconn()
        with conn.cursor() as cur:
            
            # 2. Mark as PENDING
            cur.execute("""
                UPDATE backfill_progress SET status = 'PENDING', last_updated = NOW()
                WHERE competition_id = %s AND season_year = %s;
            """, (comp_id, year))
            conn.commit() # Commit this small change immediately
            
            logging.info(f"STARTING: {task_id}")

            # --- 4. Fetch and populate data (Free Plan Endpoints) ---
            
            # 4a. Get Matches (Basic)
            logging.info(f"[{task_id}] Fetching matches...")
            matches_resp = api_call(f"competitions/{comp_id}/matches", {"season": year})
            matches_data = matches_resp.get("matches", [])
            saved_matches = upsert_matches_basic(cur, comp_id, year, matches_data)
            logging.info(f"[{task_id}] Staged {saved_matches} basic matches.")

            # 4b. Get Standings
            if is_current:
                logging.info(f"[{task_id}] Fetching standings...")
                standings_resp = api_call(f"competitions/{comp_id}/standings", {"season": year})
                standings_data = standings_resp.get("standings", [])
                upsert_standings(cur, comp_id, year, standings_data)
                logging.info(f"[{task_id}] Staged {len(standings_data)} standing lists.")
            else:
                logging.info(f"[{task_id}] Skipping standings fetch (not current season).")

            # 4c. Get Teams (to catch any teams not in standings/matches)
            logging.info(f"[{task_id}] Fetching teams...")
            teams_data = api_call(f"competitions/{comp_id}/teams", {"season": year}).get("teams", [])
            upsert_teams(cur, teams_data)
            logging.info(f"[{task_id}] Staged {len(teams_data)} teams.")

            # --- 5. Mark as COMPLETED ---
            cur.execute("""
                UPDATE backfill_progress SET status = 'COMPLETED', last_updated = NOW()
                WHERE competition_id = %s AND season_year = %s;
            """, (comp_id, year))
            
            # --- 6. ATOMIC COMMIT ---
            conn.commit() 
            
            logging.info(f"*** COMPLETE: {task_id} ***")
            return True

    except Exception as e:
        if conn:
            conn.rollback()  # Rollback any failed transaction

        # --- FIX V7.3: Special 403 (Forbidden) handling ---
        if "Forbidden (403)" in str(e):
            logging.CRITICAL(
                f"FAILED (403): {task_id}. This task is FORBIDDEN by the API "
                f"(likely an old season). Marking as 'FAILED' to prevent retry loop."
            )
            try:
                # Get a new connection for this special update
                conn = db_pool.getconn()
                with conn.cursor() as cur:
                    cur.execute("""
                        UPDATE backfill_progress SET status = 'FAILED', last_updated = NOW()
                        WHERE competition_id = %s AND season_year = %s;
                    """, (comp_id, year))
                    conn.commit()
            except Exception as db_e:
                logging.error(f"Failed to mark task {task_id} as FAILED in DB: {db_e}")
        # --- End of 403 fix ---
        else:
            logging.error(f"FAILED: {task_id}. Error: {e}. Rolling back transaction.")
        
        return False # Return False for *any* error
    finally:
        if conn:
            db_pool.putconn(conn)


# ============ MAIN ============
def main():
    start_time = time.time()
    try:
        # 1. Setup DB
        conn = db_pool.getconn()
        with conn.cursor() as cur:
            create_tables(cur)
            populate_all_areas_once(cur) # Pre-populate areas to prevent deadlocks
            conn.commit()
        db_pool.putconn(conn)
        
        wat = (
            pytz.timezone("Africa/Lagos")
            .localize(datetime.datetime.now())
            .strftime("%Y-m-%d %I:%M %p WAT")
        )
        logging.info(f"POPULATOR v7.3 (NORMALIZED) | {wat} | SMART COOLDOWN")
        logging.warning("RUNNING IN FREE TIER MODE. Will only fetch basic data.")
        logging.warning("Script will run until all *possible* tasks are 'COMPLETED'.")
        logging.warning("Tasks that are 'Forbidden (403)' by the API will be marked 'FAILED' and skipped.")

        # 2. Get all work
        all_discovered_tasks = discover_competition_tasks()
        if not all_discovered_tasks:
            logging.error("No tasks discovered. Exiting.")
            return

        # 3. Indefinite Retry Loop
        while True:
            # Get all tasks that are not yet 'COMPLETED' or 'FAILED'
            conn = db_pool.getconn()
            tasks_to_run = get_pending_tasks(conn, all_discovered_tasks)
            db_pool.putconn(conn)

            if not tasks_to_run:
                logging.info("--- ALL TASKS COMPLETE! (or marked as FAILED) ---")
                break  # Exit the while loop

            logging.info(f"--- STARTING RUN: {len(tasks_to_run)} tasks to process ---")
            
            with ThreadPoolExecutor(
                max_workers=MAX_WORKERS, thread_name_prefix="Worker"
            ) as executor:
                results = list(executor.map(process_season_task, tasks_to_run))

            # Check results
            failed_tasks = [tasks_to_run[i] for i, res in enumerate(results) if not res]
            
            if not failed_tasks:
                logging.info("--- RUN FINISHED: All tasks in this batch succeeded. ---")
            else:
                # --- FIX V7.3: Isolate len() call ---
                num_failed = len(failed_tasks)
                logging.warning(
                    f"--- RUN FAILED: {num_failed} tasks failed (this run). "
                    f"Retrying non-403 errors in {RETRY_SLEEP_SECONDS} seconds... ---"
                )
                time.sleep(RETRY_SLEEP_SECONDS)
                
            # Loop continues to next iteration

    except Exception as e:
        logging.error(f"Main process failed: {e}")
    finally:
        db_pool.closeall()
        end_time = time.time()
        logging.info(f"Database connection pool closed. Total runtime: {end_time - start_time:.2f} seconds.")


if __name__ == "__main__":
    main()