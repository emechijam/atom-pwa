# populator.py v3.1 (Fixes DB and Timeout Errors)
"""
THE FORERUNNER (v3.1)

Mission:
1. Backfill historical data from two API sources, respecting free plan limits.
2. Source A: football-data.co.uk API (FD)
   - Fetches 2023-2025 data for the 13 free leagues.
3. Source B: API-Football API (AS)
   - Fetches 2021-2023 data for popular leagues.
   - Designed to be run MANUALLY or infrequently (e.g., once a week).
   - Uses a *separate* call budget from the daily 'sync.py'.
4. Translates ALL data using 'mapping.json' before inserting.
5. Populates all tables: countries, leagues, teams, fixtures, etc.

V3.1 Changes:
- DB FIX: Renamed all calls to `db_utils` to match the correct functions:
  - `get_db_connection` -> `get_connection`
  - `release_db_connection` -> `release_connection`
  - `close_db_pool` -> `close_all_connections`
- TIMEOUT FIX: Added a 15-second timeout to `fd_api_request` to prevent
  indefinite hangs on `ConnectTimeoutError`.
- RATE LIMIT FIX: Increased `as_api_request` sleep time to 7s to
  better respect the 10/min rate limit.
"""

import os
import re
import time
import pytz
import logging
import requests
import datetime
import json
import sys
from psycopg2.extras import execute_values
from dotenv import load_dotenv
from concurrent.futures import ThreadPoolExecutor, as_completed
from dateutil import parser as date_parser

# Import database utilities
import db_utils

# ============ CONFIG & LOGGING ============
load_dotenv()
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(threadName)s] [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

# --- API Keys ---
# football-data.co.uk API Key
FD_API_KEY = os.getenv("FD_API_KEY", "") 
# API-Football Key
AS_API_KEY = os.getenv("AS_API_KEY", "5c447790790568e2c4178ef898da698e")

# --- API Endpoints ---
FD_API_URL = "https://api.football-data.org/v4"
AS_API_URL = "https://v3.football.api-sports.io"

# --- Year Range for Backfill ---
FD_SEASONS = [2023, 2024, 2025] # Per user request
AS_SEASONS = [2021, 2022, 2023] # Per user request

# --- API-Football Leagues to Backfill ---
# Popular leagues NOT fully covered by football-data.co.uk free tier
# or the CSVs. (Example: MLS, Brazil Serie A, etc.)
# We use the API-Football IDs directly here.
AS_LEAGUE_IDS_TO_BACKFILL = [
    71,  # Brazil Serie A
    253, # USA MLS
    128, # Argentina Liga Profesional
    262, # Mexico Liga MX
    98,  # Japan J1 League
]

# --- Other Config ---
MAPPING_FILE = "mapping.json"
LEAGUE_MAP = {}
TEAM_MAP = {}
MAX_WORKERS = 5
REQUEST_DELAY = 7 # 60s / 10 reqs = 6s. Add 1s buffer.

# ============ API HELPERS ============

def fd_api_request(endpoint):
    """Makes a request to the football-data.org API."""
    url = f"{FD_API_URL}/{endpoint}"
    headers = {'X-Auth-Token': FD_API_KEY}
    try:
        time.sleep(REQUEST_DELAY) # Rate limit
        # FIX v3.1: Added 15s timeout to prevent hangs
        response = requests.get(url, headers=headers, timeout=15)
        response.raise_for_status()
        return response.json()
    except Exception as e:
        logging.error(f"[FD_API] Failed to fetch {url}: {e}")
        return None

def as_api_request(endpoint, params):
    """Makes a request to the api-sports.io API."""
    url = f"{AS_API_URL}/{endpoint}"
    headers = {'x-apisports-key': AS_API_KEY}
    try:
        # FIX v3.1: Increased sleep to 7s to stay under 10 req/min
        time.sleep(7) 
        response = requests.get(url, headers=headers, params=params, timeout=15)
        response.raise_for_status()
        data = response.json()
        if data.get('errors'):
            logging.error(f"[AS_API] Error: {data['errors']}")
            return None
        return data.get('response', [])
    except Exception as e:
        logging.error(f"[AS_API] Failed to fetch {url}: {e}")
        return None

# ============ MAPPING & TRANSLATION ============

def load_mappings():
    """Loads the mapping.json file."""
    global LEAGUE_MAP, TEAM_MAP
    try:
        with open(MAPPING_FILE, 'r') as f:
            mappings = json.load(f)
        LEAGUE_MAP = mappings.get("leagues", {})
        TEAM_MAP = mappings.get("teams", {})
        if not LEAGUE_MAP or not TEAM_MAP:
            logging.error(f"FATAL: '{MAPPING_FILE}' is missing data.")
            sys.exit(1)
        logging.info(f"Mappings loaded: {len(LEAGUE_MAP)} leagues, {len(TEAM_MAP)} teams.")
    except Exception as e:
        logging.error(f"FATAL: Could not load '{MAPPING_FILE}': {e}")
        sys.exit(1)

def get_league_id(fd_code):
    """Translates a football-data code (e.g., 'E0') to an API-Football ID."""
    return LEAGUE_MAP.get(fd_code, {}).get("api_football_id")

def get_team_id_by_name(fd_name):
    """Translates a football-data name (e.g., 'Arsenal') to an API-Football ID."""
    return TEAM_MAP.get(fd_name, {}).get("api_football_id")

def get_team_id_by_fd_id(fd_team_id, conn):
    """
    Finds an API-Football team ID using the football-data.org team ID.
    This requires a new mapping.
    For now, we will try to match by name from the API response.
    """
    # This is a gap. The FD API returns team IDs, not names.
    # We need to fetch team details from FD API.
    # `GET /v4/teams/{fd_team_id}`
    # This is too many calls.
    #
    # We will assume the `matches` endpoint from FD
    # includes team *names*.
    pass

# ============ DATABASE UPSERT LOGIC ============
# (These functions are more detailed than in csv_populator)

def upsert_country(cursor, name, code=None, flag=None):
    sql = "INSERT INTO countries (name, code, flag_url) VALUES (%s, %s, %s) ON CONFLICT (name) DO UPDATE SET code = EXCLUDED.code, flag_url = EXCLUDED.flag_url;"
    cursor.execute(sql, (name, code, flag))

def upsert_league(cursor, data):
    sql = """
    INSERT INTO leagues (league_id, name, type, logo_url, country_name)
    VALUES (%(id)s, %(name)s, %(type)s, %(logo)s, %(country)s)
    ON CONFLICT (league_id) DO UPDATE SET
        name = EXCLUDED.name,
        type = EXCLUDED.type,
        logo_url = EXCLUDED.logo_url,
        country_name = EXCLUDED.country_name;
    """
    cursor.execute(sql, data)

def upsert_team(cursor, data):
    sql = """
    INSERT INTO teams (team_id, name, code, country, founded, national, logo_url, venue_id)
    VALUES (%(id)s, %(name)s, %(code)s, %(country)s, %(founded)s, %(national)s, %(logo)s, %(venue_id)s)
    ON CONFLICT (team_id) DO UPDATE SET
        name = EXCLUDED.name,
        code = EXCLUDED.code,
        country = EXCLUDED.country,
        founded = EXCLUDED.founded,
        logo_url = EXCLUDED.logo_url,
        venue_id = EXCLUDED.venue_id;
    """
    cursor.execute(sql, data)

def upsert_venue(cursor, data):
    sql = """
    INSERT INTO venues (venue_id, name, address, city, country, capacity, surface, image_url)
    VALUES (%(id)s, %(name)s, %(address)s, %(city)s, %(country)s, %(capacity)s, %(surface)s, %(image)s)
    ON CONFLICT (venue_id) DO UPDATE SET
        name = EXCLUDED.name,
        address = EXCLUDED.address,
        city = EXCLUDED.city,
        country = EXCLUDED.country,
        capacity = EXCLUDED.capacity,
        surface = EXCLUDED.surface,
        image_url = EXCLUDED.image_url;
    """
    cursor.execute(sql, data)

def upsert_season(cursor, year):
    sql = "INSERT INTO seasons (year) VALUES (%s) ON CONFLICT (year) DO NOTHING;"
    cursor.execute(sql, (year,))

def upsert_fixture_batch(conn, fixtures_data):
    """
    Bulk inserts/updates fixtures into the database.
    This uses the REAL API-Football fixture_id.
    """
    sql = """
    INSERT INTO fixtures (
        fixture_id, league_id, season_year, date, timestamp,
        referee, timezone, venue_id,
        status_long, status_short, elapsed,
        home_team_id, away_team_id,
        home_winner, away_winner,
        goals_home, goals_away,
        score_ht_home, score_ht_away,
        score_ft_home, score_ft_away,
        score_et_home, score_et_away,
        score_pen_home, score_pen_away
    ) VALUES %s
    ON CONFLICT (fixture_id) DO UPDATE SET
        date = EXCLUDED.date,
        timestamp = EXCLUDED.timestamp,
        referee = EXCLUDED.referee,
        status_long = EXCLUDED.status_long,
        status_short = EXCLUDED.status_short,
        elapsed = EXCLUDED.elapsed,
        home_winner = EXCLUDED.home_winner,
        away_winner = EXCLUDED.away_winner,
        goals_home = EXCLUDED.goals_home,
        goals_away = EXCLUDED.goals_away,
        score_ht_home = EXCLUDED.score_ht_home,
        score_ht_away = EXCLUDED.score_ht_away,
        score_ft_home = EXCLUDED.score_ft_home,
        score_ft_away = EXCLUDED.score_ft_away,
        score_et_home = EXCLUDED.score_et_home,
        score_et_away = EXCLUDED.score_et_away,
        score_pen_home = EXCLUDED.score_pen_home,
        score_pen_away = EXCLUDED.score_pen_away;
    """
    
    values_list = []
    for f in fixtures_data:
        values_list.append((
            f['fixture_id'], f['league_id'], f['season_year'], f['date'], f['timestamp'],
            f.get('referee'), f.get('timezone'), f.get('venue_id'),
            f.get('status_long'), f.get('status_short'), f.get('elapsed'),
            f.get('home_team_id'), f.get('away_team_id'),
            f.get('home_winner'), f.get('away_winner'),
            f.get('goals_home'), f.get('goals_away'),
            f.get('score_ht_home'), f.get('score_ht_away'),
            f.get('score_ft_home'), f.get('score_ft_away'),
            f.get('score_et_home'), f.get('score_et_away'),
            f.get('score_pen_home'), f.get('score_pen_away')
        ))

    if not values_list:
        return

    try:
        with conn.cursor() as cursor:
            execute_values(cursor, sql, values_list)
        logging.info(f"Successfully upserted {len(values_list)} fixtures.")
    except Exception as e:
        logging.error(f"Failed to bulk upsert fixtures: {e}")
        raise # Re-raise to trigger rollback

# ============ SOURCE A: API-Football (AS) ============

def process_as_fixture_response(response, season_year):
    """Transforms AS API fixture data into our DB schema."""
    fixtures_data = []
    venues_to_upsert = {}
    
    for item in response:
        f = item['fixture']
        l = item['league']
        t = item['teams']
        g = item['goals']
        s = item['score']
        v = f.get('venue')
        
        # 1. Prepare Venue (if it exists)
        venue_id = None
        if v and v.get('id'):
            venue_id = v['id']
            if venue_id not in venues_to_upsert:
                venues_to_upsert[venue_id] = {
                    'id': v['id'],
                    'name': v.get('name'),
                    'address': None,
                    'city': v.get('city'),
                    'country': None, # AS fixture response doesn't include venue country
                    'capacity': None,
                    'surface': None,
                    'image': None
                }

        # 2. Prepare Fixture
        dt = date_parser.parse(f['date'])
        
        fixture_data = {
            'fixture_id': f['id'],
            'league_id': l['id'],
            'season_year': season_year,
            'date': dt,
            'timestamp': f.get('timestamp'),
            'referee': f.get('referee'),
            'timezone': f.get('timezone'),
            'venue_id': venue_id,
            'status_long': f['status']['long'],
            'status_short': f['status']['short'],
            'elapsed': f['status'].get('elapsed'),
            'home_team_id': t['home'].get('id'),
            'away_team_id': t['away'].get('id'),
            'home_winner': t['home'].get('winner'),
            'away_winner': t['away'].get('winner'),
            'goals_home': g.get('home'),
            'goals_away': g.get('away'),
            'score_ht_home': s['halftime'].get('home'),
            'score_ht_away': s['halftime'].get('away'),
            'score_ft_home': s['fulltime'].get('home'),
            'score_ft_away': s['fulltime'].get('away'),
            'score_et_home': s['extratime'].get('home'),
            'score_et_away': s['extratime'].get('away'),
            'score_pen_home': s['penalty'].get('home'),
            'score_pen_away': s['penalty'].get('away')
        }
        fixtures_data.append(fixture_data)
        
    return fixtures_data, list(venues_to_upsert.values())

def run_as_backfill(league_id, season_year):
    """
    Task: Fetches and populates all fixtures for a given
    API-Football league and season.
    """
    logging.info(f"[AS_Backfill] STARTING: League {league_id}, Season {season_year}")
    conn = None
    try:
        # 1. Fetch Fixtures
        fixtures_response = as_api_request('fixtures', {'league': league_id, 'season': season_year})
        if not fixtures_response:
            logging.warning(f"[AS_Backfill] No fixtures found for {league_id} / {season_year}.")
            return
            
        logging.info(f"[AS_Backfill] Found {len(fixtures_response)} fixtures.")
        
        # 2. Transform Data
        fixtures_to_upsert, venues_to_upsert = process_as_fixture_response(fixtures_response, season_year)
        
        # 3. Get DB Connection
        # FIX v3.1: Use correct function name
        conn = db_utils.get_connection()
        if not conn:
             logging.error(f"[AS_Backfill] Could not get DB connection for {league_id} / {season_year}.")
             return

        with conn.cursor() as cursor:
            # 4. Upsert Venues first (Foreign Key)
            for v_data in venues_to_upsert:
                upsert_venue(cursor, v_data)
            logging.info(f"[AS_Backfill] Upserted {len(venues_to_upsert)} venues.")
            
            # 5. Upsert Season (Foreign Key)
            upsert_season(cursor, season_year)

        # 6. Bulk Upsert Fixtures
        upsert_fixture_batch(conn, fixtures_to_upsert)
        
        # FIX v3.1: Use correct function name
        conn.commit() # Commit transaction
        db_utils.release_connection(conn)
        logging.info(f"[AS_Backfill] SUCCESS: League {league_id}, Season {season_year}")
        
    except Exception as e:
        logging.error(f"[AS_Backfill] FAILED: League {league_id}, Season {season_year}: {e}")
        if conn:
            # FIX v3.1: Use correct function name
            conn.rollback() # Rollback on error
            db_utils.release_connection(conn)

# ============ SOURCE B: football-data.org (FD) ============

def process_fd_match_response(response, fd_league_code, season_year):
    """Transforms FD API match data into our DB schema."""
    matches = response.get('matches', [])
    fixtures_data = []
    
    # Get the API-Football league_id from mapping
    as_league_id = get_league_id(fd_league_code)
    if not as_league_id:
        logging.error(f"[FD_Process] No AS_League_ID for {fd_league_code}. Skipping.")
        return []

    for match in matches:
        # 1. Get Team IDs
        home_team_name = match.get('homeTeam', {}).get('name')
        away_team_name = match.get('awayTeam', {}).get('name')
        
        as_home_team_id = get_team_id_by_name(home_team_name)
        as_away_team_id = get_team_id_by_name(away_team_name)
        
        if not as_home_team_id or not as_away_team_id:
            logging.warning(f"[FD_Process] Skipping match: Cannot map teams '{home_team_name}' or '{away_team_name}'.")
            continue
            
        # 2. Get Date
        dt = date_parser.parse(match['utcDate'])
        
        # 3. Get Score & Status
        status = match.get('status')
        score = match['score']
        
        if status != "FINISHED":
            continue # Only care about finished matches for backfill
            
        # 4. Generate stable, negative fixture_id
        stable_key = f"{as_league_id}{season_year}{as_home_team_id}{as_away_team_id}{dt.strftime('%Y-%m-%d')}"
        fixture_id_hash = 0
        for char in stable_key:
            fixture_id_hash = (fixture_id_hash * 31 + ord(char)) & 0xFFFFFFFF
        fixture_id = - (fixture_id_hash % 2147483647)
        
        # 5. Prepare Fixture
        fixture_data = {
            'fixture_id': fixture_id,
            'league_id': as_league_id,
            'season_year': season_year,
            'date': dt,
            'timestamp': int(dt.timestamp()),
            'referee': match.get('referee', {}).get('name'), # FD provides referee
            'timezone': 'UTC',
            'venue_id': None, # FD API doesn't provide venue ID
            'status_long': "Match Finished",
            'status_short': "FT",
            'elapsed': 90,
            'home_team_id': as_home_team_id,
            'away_team_id': as_away_team_id,
            'home_winner': True if score['winner'] == 'HOME_TEAM' else False,
            'away_winner': True if score['winner'] == 'AWAY_TEAM' else False,
            'goals_home': score['fullTime'].get('home'),
            'goals_away': score['fullTime'].get('away'),
            'score_ht_home': score['halfTime'].get('home'),
            'score_ht_away': score['halfTime'].get('away'),
            'score_ft_home': score['fullTime'].get('home'),
            'score_ft_away': score['fullTime'].get('away'),
            'score_et_home': score['extraTime'].get('home'),
            'score_et_away': score['extraTime'].get('away'),
            'score_pen_home': score['penalties'].get('home'),
            'score_pen_away': score['penalties'].get('away')
        }
        fixtures_data.append(fixture_data)
        
    return fixtures_data

def run_fd_backfill(fd_league_code, season_year):
    """
    Task: Fetches and populates all fixtures for a given
    football-data.org league and season.
    """
    logging.info(f"[FD_Backfill] STARTING: League {fd_league_code}, Season {season_year}")
    conn = None
    try:
        # 1. Fetch Fixtures
        # FD API uses 'season' param as the start year
        fixtures_response = fd_api_request(f'competitions/{fd_league_code}/matches?season={season_year}')
        
        if not fixtures_response or not fixtures_response.get('matches'):
            logging.warning(f"[FD_Backfill] No matches found for {fd_league_code} / {season_year}.")
            return
        
        logging.info(f"[FD_Backfill] Found {len(fixtures_response['matches'])} matches.")
        
        # 2. Transform Data
        fixtures_to_upsert = process_fd_match_response(fixtures_response, fd_league_code, season_year)
        
        if not fixtures_to_upsert:
            logging.warning(f"[FD_Backfill] No mappable matches found for {fd_league_code} / {season_year}.")
            return

        # 3. Get DB Connection
        # FIX v3.1: Use correct function name
        conn = db_utils.get_connection()
        if not conn:
             logging.error(f"[FD_Backfill] Could not get DB connection for {fd_league_code} / {season_year}.")
             return
        
        with conn.cursor() as cursor:
            # 4. Upsert Season (Foreign Key)
            upsert_season(cursor, season_year)

        # 5. Bulk Upsert Fixtures
        # Note: We use the *same* function as AS, since we transformed
        # the data into the standard format.
        upsert_fixture_batch(conn, fixtures_to_upsert)
        
        # FIX v3.1: Use correct function name
        conn.commit() # Commit transaction
        db_utils.release_connection(conn)
        logging.info(f"[FD_Backfill] SUCCESS: League {fd_league_code}, Season {season_year}")
        
    except Exception as e:
        logging.error(f"[FD_Backfill] FAILED: League {fd_league_code}, Season {season_year}: {e}")
        if conn:
            # FIX v3.1: Use correct function name
            conn.rollback() # Rollback on error
            db_utils.release_connection(conn)


# ============ MAIN EXECUTION ============

def main():
    logging.info("--- Populator (Forerunner) v3.0 Starting ---")
    if not FD_API_KEY:
        logging.warning("FD_API_KEY not set. Skipping football-data.org backfill.")
    if not AS_API_KEY:
        logging.warning("AS_API_KEY not set. Skipping API-Football backfill.")
        
    load_mappings()
    
    # --- Create Task List ---
    tasks = []
    
    # 1. Add football-data.org tasks
    if FD_API_KEY:
        # These are the 13 leagues in the FD free tier
        fd_free_leagues = [
            'PL', 'CL', 'BL1', 'SA', 'D1', 'FL1', 'PPL',
            'EC', 'WC', 'ELC', 'E0', 'E1', 'SP1'
        ]
        # Let's filter by leagues we have mappings for
        fd_leagues_to_run = [lc for lc in fd_free_leagues if lc in LEAGUE_MAP]
        
        for season in FD_SEASONS:
            for league_code in fd_leagues_to_run:
                tasks.append((run_fd_backfill, league_code, season))
                
    # 2. Add API-Football tasks
    if AS_API_KEY:
        for season in AS_SEASONS:
            for league_id in AS_LEAGUE_IDS_TO_BACKFILL:
                tasks.append((run_as_backfill, league_id, season))

    logging.info(f"Generated {len(tasks)} backfill tasks.")

    # --- Initialize DB Pool ---
    try:
        db_utils.init_connection_pool()
    except Exception as e:
        logging.critical(f"Failed to initialize DB Pool: {e}")
        return

    # --- Run Tasks Concurrently ---
    with ThreadPoolExecutor(max_workers=MAX_WORKERS, thread_name_prefix="BackfillWorker") as executor:
        futures = {executor.submit(task[0], *task[1:]): task for task in tasks}
        
        for future in as_completed(futures):
            task_info = futures[future]
            try:
                future.result()  # Wait for task to complete
            except Exception as e:
                logging.error(f"Task {task_info[0].__name__}{task_info[1:]} failed: {e}")

    logging.info("--- Populator Finished ---")

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        logging.error(f"FATAL: Main execution failed: {e}")
    finally:
        # FIX v3.1: Use correct function name
        db_utils.close_all_connections()