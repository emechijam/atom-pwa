# sync.py v2.6 - Live Poller & Predictor Trigger
#
# WHAT'S NEW (v2.6):
# - RULE CHANGE: The polling window (DAYS_AHEAD) is now set to 5 years (1825 days)
#   to ensure ALL future/timed matches for the entire season are fetched,
#   as requested, regardless of how far out they are scheduled.
#
# WHAT'S NEW (v2.5):
# - BUG FIX (UnicodeDecodeError): Made the subprocess call more robust.

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

# --- Polling Configuration ---
POLL_INTERVAL_MINUTES = 5
DAYS_BEHIND = 2
# --- START OF FIX: Extend DAYS_AHEAD to cover end-of-season matches (5 years) ---
DAYS_AHEAD = 1825  
# --- END OF FIX ---
MAX_WORKERS = 5
VERSION = "v2.6" 
# -----------------------------

# --- Free Tier Competition Codes (Must match populator.py) ---
FREE_CODES = ['BSA','BL1', 'CL', 'DED', 'EC', 'FL1', 'PD', 'PL', 'PPL', 'SA', 'WC', 'ELC', 'CLI']
# -----------------------------------

MAX_DB_CONNECTIONS = 10

# ============ CONNECT ============
try:
    db_url = os.getenv("DATABASE_URL")
    if not db_url:
        logging.error("DATABASE_URL not found. Check .env file or Streamlit Secrets.")
        exit(1)

    if db_url.startswith("postgresql+psycopg://"):
        logging.warning("DSN prefix 'postgresql+psycopg://' found, correcting to 'postgresql://'.")
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

http_session = requests.Session()


class KeyRotator:
    """Manages API keys using a PriorityQueue to respect per-key rate limits."""
    def __init__(self, keys: List[str]):
        self.queue = PriorityQueue()
        self.lock = (
            threading.Lock()
        )
        if not keys:
            logging.error("No API keys provided to KeyRotator.")
            exit(1)
        for key in keys:
            self.queue.put((0, key))
        logging.info(f"LOADED {len(keys)} KEYS — SMART COOLDOWN MODE!")

    def get_next(self) -> str:
        """Gets the soonest-available key, waiting if necessary."""
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
        """A successful call. Put key back on cooldown for 6.5s."""
        next_use = time.time() + 6.5
        self.queue.put((next_use, key))

    def penalize(self, key: str):
        """A rate-limit (429) or other major error occurred."""
        with self.lock:
            logging.warning(f"PENALIZED → {key[:8]}... | Cooldown 70s")
        next_use = time.time() + 70.0
        self.queue.put((next_use, key))


rotator = KeyRotator(API_KEYS)


def api_call(endpoint: str, params: Optional[Dict] = None) -> Dict[str, Any]:
    """Makes an API call using the smart key rotator."""
    key = ""
    try:
        key = rotator.get_next()
        r = http_session.get(
            f"{BASE_URL}/{endpoint}",
            headers={"X-Auth-Token": key},
            params=params,
            timeout=20,
        )
        
        if r.status_code == 400:
            logging.error(f"API call failed: 400 Bad Request for {r.url}")
            rotator.penalize(key)
            raise Exception(f"400 Bad Request for {r.url} (check filters)")
            
        r.raise_for_status()
        
        try:
            data = r.json()
        except requests.exceptions.JSONDecodeError:
            logging.warning(f"API call to {endpoint} returned {r.status_code} but no valid JSON.")
            data = {}
            
        rotator.release(key)
        return data

    except requests.exceptions.HTTPError as http_err:
        if http_err.response.status_code == 403:
            logging.warning(f"API Call Forbidden (403) for {endpoint}. Check your plan.")
        elif http_err.response.status_code == 429:
            logging.warning(f"API Call Rate-Limited (429) for {endpoint}.")
        else:
            logging.error(f"HTTP Error: {http_err}")
            
        if key:
            rotator.penalize(key)
        raise http_err
        
    except Exception as e:
        logging.error(f"API call to {endpoint} failed: {e}")
        if key:
             rotator.penalize(key)
        raise e


# ============ SMART UPSERTS (Copied from Populator) ============

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
        execute_values(cur, sql, values, page_size=100)

def upsert_matches_from_sync(cur, matches_data: List[Dict]):
    """
    Upserts a list of basic match info from the /matches endpoint.
    """
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
            
        if home_team:
            teams_to_upsert.append(home_team)
        if away_team:
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


# ============ NEW HELPER FUNCTION ============

def get_competition_id_map() -> Dict[str, int]:
    """
    Queries the local database to map FREE_CODES (e.g., 'PL')
    to their API competition_id (e.g., 2021).
    """
    logging.info("Fetching competition ID map from local database...")
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
                    "Run populator.py to ensure the competitions table is full."
                )
            
            logging.info(f"Found {len(comp_map)} competitions to poll.")
            return comp_map
            
    except Exception as e:
        logging.error(f"Failed to get competition ID map: {e}")
        return {}
    finally:
        if conn:
            db_pool.putconn(conn)


# ============ UPDATED WORKER FUNCTION (v2.4 FIX) ============

def poll_competition(comp_code: str, comp_id: int, date_from: str, date_to: str) -> int:
    """
    Worker function to fetch and sync matches for a *single* competition.
    Returns the number of matches synced.
    """
    conn = None
    try:
        logging.info(f"Polling matches for {comp_code} (ID: {comp_id})...")
        
        params = {
            "dateFrom": date_from,
            "dateTo": date_to
        }
        
        endpoint = f"competitions/{comp_id}/matches"
        
        response = api_call(endpoint, params=params)
        matches = response.get("matches", [])
        
        if not matches:
            logging.info(f"No matches found for {comp_code} in this window.")
            return 0

        conn = db_pool.getconn()
        with conn.cursor() as cur: 
            count = upsert_matches_from_sync(cur, matches)
            conn.commit()
            logging.info(f"Successfully upserted {count} matches for {comp_code}.")
            return count
            
    except Exception as e:
        logging.error(f"Failed to poll competition {comp_code}. Error: {e}")
        if conn:
            conn.rollback()
        return 0
    finally:
        if conn:
            db_pool.putconn(conn)


# ============ MAIN POLLING LOOP ============
def main():
    logging.info(f"--- SYNC POLLER (v{VERSION}) STARTING ---") 
    
    comp_map = get_competition_id_map()
    if not comp_map:
        logging.error("No competitions to poll. Exiting.")
        logging.error("Please run populator.py at least once to fill the 'competitions' table.")
        return

    while True:
        start_time = time.time()
        total_matches_synced = 0
        
        try:
            today = datetime.datetime.now(UTC)
            date_from = (today - datetime.timedelta(days=DAYS_BEHIND)).strftime("%Y-%m-%d")
            # --- START OF FIX: Use DAYS_AHEAD (1825 days) for date_to ---
            date_to = (today + datetime.timedelta(days=DAYS_AHEAD)).strftime("%Y-%m-%d")
            # --- END OF FIX ---
            
            logging.info(f"Polling matches from {date_from} to {date_to} for {len(comp_map)} competitions...")
            
            with ThreadPoolExecutor(max_workers=MAX_WORKERS, thread_name_prefix="SyncWorker") as executor:
                futures = {
                    executor.submit(poll_competition, code, comp_id, date_from, date_to): code
                    for code, comp_id in comp_map.items()
                }
                
                for future in futures:
                    try:
                        matches_count = future.result()
                        total_matches_synced += matches_count
                    except Exception as e:
                        comp_code = futures[future]
                        logging.error(f"Thread for {comp_code} failed: {e}")

            if total_matches_synced == 0:
                logging.info("No new/updated matches found. Database is up-to-date.")
                logging.info("Predictor not needed.")
            else:
                logging.info(f"Successfully synced a total of {total_matches_synced} matches.")
                
                # --- AUTOMATICALLY RUN PREDICTOR ---
                logging.info("Sync found matches. Triggering predictor.py...")
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
                    logging.error(f"predictor.py failed with exit code {e.returncode}.")
                    logging.error(f"PREDICTOR STDOUT: {e.stdout}")
                    logging.error(f"PREDICTOR STDERR: {e.stderr}")
                except FileNotFoundError:
                    logging.error("ERROR: predictor.py not found in the current directory.")
                # -------------------------------------------

        except Exception as e:
            logging.error(f"Main poll cycle failed: {e}")
        finally:
            end_time = time.time()
            logging.info(f"Poll cycle finished in {end_time - start_time:.2f} seconds.")
            
            logging.info(f"Sleeping for {POLL_INTERVAL_MINUTES} minutes...")
            time.sleep(POLL_INTERVAL_MINUTES * 60)
            

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        logging.info("--- SYNC POLLER STOPPING (KeyboardInterrupt) ---")
        if 'db_pool' in globals():
            db_pool.closeall()
            logging.info("Database connection pool closed.")
    except Exception as e:
        logging.error(f"--- SYNC POLLER CRASHED: {e} ---")
        if 'db_pool' in globals():
            db_pool.closeall()
            logging.info("Database connection pool closed.")