# sync.py v4.14 - Enrichment Poller and Predictor Trigger
"""
THE ENRICHER (v4.14 - ASYNC API CALLS)

WHAT'S NEW (v4.14):
- ASYNC: Replaced ThreadPoolExecutor and 'requests' with asyncio/aiohttp for non-blocking API calls.
- CONNECTION MANAGEMENT: DB connection acquisition/release is managed by the async worker functions.
- CONFIG: SYNC_INTERVAL_SECONDS from .env if set.
- RETAINED: v4.13 fixes (Priority League season determination, robust upserts).
"""

import os
import time
import pytz
import logging
import aiohttp
import asyncio
import datetime as dt
import json
import sys
import subprocess
import re
import math
from datetime import UTC
from psycopg2.extras import execute_values, RealDictCursor
from dotenv import load_dotenv
from typing import List, Tuple, Dict, Any, Optional, Set

# Import database utilities
import db_utils 

# ============ CONFIG & LOGGING ============
load_dotenv()
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(threadName)s] [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

# API Config
AS_API_KEY = os.getenv("AS_API_KEY")
AS_FIXTURES_URL = "https://v3.football.api-sports.io/fixtures"
AS_TEAMS_URL = "https://v3.football.api-sports.io/teams"
AS_STANDINGS_URL = "https://v3.football.api-sports.io/standings"

# Poller Config
SYNC_INTERVAL_SECONDS = int(os.getenv("SYNC_INTERVAL_SECONDS", 1800))  # Default 30min
ENRICHMENT_CHECK_INTERVAL_SECONDS = 60 * 60
MAX_WORKERS = 4 
FIXTURE_UPSERT_CHUNK_SIZE = 250 
MAPPING_FILE = "mapping.json"

# Enrichment Config Constants from db_utils
COOLDOWN_HOURS = db_utils.ENRICHMENT_COOLDOWN_HOURS
BATCH_SIZE = db_utils.ENRICHMENT_BATCH_SIZE * 2 # 20 leagues, 2 calls per league (Team + Standings)

# API Headers (used by aiohttp.ClientSession)
API_HEADERS = {
    "x-apisports-key": AS_API_KEY,
    "Content-Type": "application/json",
}

# Date utility
TIMEZONE = pytz.timezone("UTC") # API-Football dates are typically UTC

# Global to store priority league IDs
PRIORITY_LEAGUE_IDS: Set[int] = set()
LAST_ENRICHMENT_RUN: dt.datetime = dt.datetime.now(tz=UTC) - dt.timedelta(days=1) # Initialize to allow first run

# ============ UTILITIES ============

def chunked(iterable, n):
    """Simple internal chunker function."""
    return [iterable[i:i + n] for i in range(0, len(iterable), n)]

def load_priority_league_ids():
    """Loads league IDs marked as PRIORITY from mapping.json."""
    global PRIORITY_LEAGUE_IDS
    try:
        with open(MAPPING_FILE, 'r', encoding='utf-8') as f:
            mappings = json.load(f)
            league_map = mappings.get("leagues", {})
            for data in league_map.values():
                if "api_football_id" in data:
                    PRIORITY_LEAGUE_IDS.add(data["api_football_id"])
        logging.info(f"Loaded {len(PRIORITY_LEAGUE_IDS)} priority league IDs from {MAPPING_FILE}.")
    except FileNotFoundError:
        logging.error(f"Mapping file {MAPPING_FILE} not found. Priority leagues disabled.")
    except json.JSONDecodeError:
        logging.error(f"Could not parse {MAPPING_FILE}. Priority leagues disabled.")

def initialize_priority_status():
    """
    On startup, ensures that all leagues listed in mapping.json (PRIORITY leagues)
    are present and marked as 'PRIORITY' in the enrichment_status table.
    """
    if not PRIORITY_LEAGUE_IDS:
        return

    conn = db_utils.get_connection()
    if conn is None:
        return

    try:
        with conn.cursor() as cursor:
            logging.info(f"[DB Init] Checking and updating status for {len(PRIORITY_LEAGUE_IDS)} priority leagues...")
            
            # Use a datetime object (30 days ago) instead of None to satisfy NOT NULL constraint
            thirty_days_ago = dt.datetime.now(tz=UTC) - dt.timedelta(days=30)
            
            # Columns: (league_id, status, last_enriched_at)
            priority_values = [(lid, 'PRIORITY', thirty_days_ago) for lid in PRIORITY_LEAGUE_IDS]
            
            upsert_sql = """
                INSERT INTO enrichment_status (league_id, status, last_enriched_at)
                VALUES %s
                ON CONFLICT (league_id) DO UPDATE SET
                    status = EXCLUDED.status,
                    -- If the league was previously enriched, we reset the timestamp to force re-enrichment after 30 days
                    last_enriched_at = CASE WHEN enrichment_status.status != 'ENRICHED' OR enrichment_status.last_enriched_at < NOW() - INTERVAL '30 days' THEN EXCLUDED.last_enriched_at ELSE enrichment_status.last_enriched_at END;
            """
            
            execute_values(cursor, upsert_sql, priority_values)
            conn.commit()
            logging.info("[DB Init] Priority league statuses ensured in enrichment_status table.")
            
    except Exception as e:
        conn.rollback()
        logging.error(f"[DB Init] Failed to initialize priority league status: {e}")
    finally:
        db_utils.release_connection(conn)

async def async_get(session, url, params=None):
    """Async API fetch with retry and robust error handling."""
    for attempt in range(db_utils.MAX_RETRIES):
        try:
            async with session.get(url, params=params, timeout=db_utils.TIMEOUT_SECONDS) as response:
                
                # Check for rate limit/fatal error
                if response.status == 403:
                    logging.error(f"[API] FATAL 403: API Key issue or plan limit hit. Stopping API attempts.")
                    return None
                if response.status == 429:
                    logging.warning(f"[API] Rate limit hit (429) for {url}. Retrying in {db_utils.RETRY_SLEEP_SECONDS}s...")
                    await asyncio.sleep(db_utils.RETRY_SLEEP_SECONDS * (2 ** attempt))
                    continue # Go to next attempt
                    
                response.raise_for_status() 
                
                data = await response.json()
                
                if data.get("errors"):
                    logging.error(f"[API] API returned errors: {data.get('errors')} for {url}")
                    return None
                return data
                
        except aiohttp.ClientError as e:
            logging.warning(f"[API] Client error (attempt {attempt+1}): {e} to {url}")
        except asyncio.TimeoutError:
            logging.warning(f"[API] Request timed out (attempt {attempt+1}): {url}")
            
        if attempt < db_utils.MAX_RETRIES - 1:
            await asyncio.sleep(db_utils.RETRY_SLEEP_SECONDS * (2 ** attempt))
            
    logging.error(f"[API] Request to {url} failed after {db_utils.MAX_RETRIES} attempts.")
    return None

# ============ HIGH-FREQUENCY SYNC LOGIC (Fixtures) ============

def transform_fixture_data(fixture: Dict[str, Any]) -> Tuple[int, Dict[str, Any]]:
    """
    Transforms API data into the format needed for the fixtures table.
    Includes Foreign Keys (league_id, team_ids, season_year) required for UPSERT.
    """
    
    # 1. Extract IDs
    fixture_id = fixture['fixture']['id']
    
    # 2. Extract Status
    status = fixture['fixture']['status']
    status_short = status['short']
    status_long = status['long']
    
    # 3. Extract Goals (ensure they are integers, even if API sends None)
    goals_home = db_utils.safe_int(fixture['goals']['home'])
    goals_away = db_utils.safe_int(fixture['goals']['away'])

    # 4. Determine Winner (based on FT goals)
    home_winner = goals_home > goals_away if goals_home is not None and goals_away is not None else None
    away_winner = goals_away > goals_home if goals_home is not None and goals_away is not None else None
    
    # 5. Extract Scores (handle None for non-existent periods)
    score_ht_home = db_utils.safe_int(fixture['score']['halftime']['home'])
    score_ht_away = db_utils.safe_int(fixture['score']['halftime']['away'])
    
    # FT scores are goals_home/away (needed for final score columns)
    score_ft_home = goals_home
    score_ft_away = goals_away
    
    score_et_home = db_utils.safe_int(fixture['score']['extratime']['home'])
    score_et_away = db_utils.safe_int(fixture['score']['extratime']['away'])
    
    score_pen_home = db_utils.safe_int(fixture['score']['penalty']['home'])
    score_pen_away = db_utils.safe_int(fixture['score']['penalty']['away'])

    # 6. Calculate total goals (FT + ET)
    # Ensure goals_home/away and extra-time scores are treated as 0 if None
    total_goals_home = (goals_home or 0) + (score_et_home or 0)
    total_goals_away = (goals_away or 0) + (score_et_away or 0)
    
    # 7. Extract Foreign Keys (NEW for UPSERT)
    league_id = fixture['league']['id']
    season_year = fixture['league']['season']
    home_team_id = fixture['teams']['home']['id']
    away_team_id = fixture['teams']['away']['id']
    venue_id = fixture['fixture']['venue']['id'] if fixture['fixture']['venue'] and fixture['fixture']['venue']['id'] else None

    # 8. Package data for UPSERT
    update_data = {
        'fixture_id': fixture_id,
        'referee': fixture['fixture'].get('referee'),
        'date': fixture['fixture']['date'], # ISO string (e.g., '2025-11-14T20:00:00+00:00')
        'timestamp': fixture['fixture']['timestamp'], # Unix timestamp (integer)
        'status_long': status_long,
        'status_short': status_short,
        'elapsed': db_utils.safe_int(fixture['fixture']['status'].get('elapsed')),
        'home_winner': home_winner,
        'away_winner': away_winner,
        'goals_home': total_goals_home,
        'goals_away': total_goals_away,
        'score_ht_home': score_ht_home,
        'score_ht_away': score_ht_away,
        'score_ft_home': score_ft_home,
        'score_ft_away': score_ft_away,
        'score_et_home': score_et_home,
        'score_et_away': score_et_away,
        'score_pen_home': score_pen_home,
        'score_pen_away': score_pen_away,
        
        # New fields for UPSERT (required for initial INSERT)
        'league_id': league_id,
        'season_year': season_year,
        'home_team_id': home_team_id,
        'away_team_id': away_team_id,
        'venue_id': venue_id,
    }

    return fixture_id, update_data

def update_fixtures_db(fixtures_data: List[Dict[str, Any]], conn) -> Set[int]:
    """
    UPSERTs (Inserts or Updates) parent entities and then fixtures with schedule and result details.
    This sync function is called by the async worker and uses the provided DB connection.
    """
    if not fixtures_data:
        return set()

    cursor = conn.cursor(cursor_factory=RealDictCursor)
    updated_fixture_ids: Set[int] = set()
    
    # --- 1. Extract Parent Data and Prepare Fixture Tuples ---
    teams_to_upsert = {}    # {team_id: {data}}
    venues_to_upsert = {} # {venue_id: {data}}
    seasons_to_upsert = set() # {year}
    leagues_to_upsert = {} # {league_id: {data}}
    
    UPSERT_COLUMNS = [
        'fixture_id', 'referee', 'date', 'timestamp', 'status_long', 'status_short', 'elapsed',
        'home_winner', 'away_winner', 'goals_home', 'goals_away',
        'score_ht_home', 'score_ht_away', 'score_ft_home', 'score_ft_away',
        'score_et_home', 'score_et_away', 'score_pen_home', 'score_pen_away',
        'league_id', 'season_year', 'home_team_id', 'away_team_id', 'venue_id'
    ]

    fixture_tuples = []
    
    for fixture in fixtures_data:
        fixture_id, data = transform_fixture_data(fixture)
        
        # A. Collect Team Data
        home_team = fixture['teams']['home']
        away_team = fixture['teams']['away']
        league_country = fixture['league'].get('country')
        
        for team in [home_team, away_team]:
            team_id = team.get('id')
            if team_id and team_id not in teams_to_upsert:
                # Include placeholders for code, founded, and national to ensure all 8 columns exist
                teams_to_upsert[team_id] = {
                    'team_id': team_id,
                    'name': team.get('name'),
                    'code': None, # Placeholder for FIX 2
                    'country': league_country, 
                    'founded': None, # Placeholder for FIX 2
                    'national': None, # Placeholder for FIX 2
                    'logo_url': team.get('logo'),
                    # Only map venue if the team is the home team
                    'venue_id': data.get('venue_id') if data.get('home_team_id') == team_id else None
                }

        # B. Collect Venue Data
        venue = fixture['fixture']['venue']
        venue_id = data.get('venue_id')
        if venue_id and venue_id not in venues_to_upsert:
            venues_to_upsert[venue_id] = {
                'venue_id': venue_id,
                'name': venue.get('name'),
                'city': venue.get('city'),
                'country': league_country, 
            }
        
        # C. Collect Season/League Data
        season_year = data['season_year']
        league_id = data['league_id']
        seasons_to_upsert.add(season_year)
        
        league = fixture['league']
        if league_id and league_id not in leagues_to_upsert:
             leagues_to_upsert[league_id] = {
                'league_id': league_id,
                'name': league.get('name'),
                'type': league.get('type'),
                'logo_url': league.get('logo'),
                'country_name': league_country,
            }
            
        # D. Prepare fixture tuple for bulk UPSERT
        fixture_tuples.append(tuple(data[col] for col in UPSERT_COLUMNS))

    # --- 2. JIT UPSERT PARENT ENTITIES ---
    try:
        # 2a. Seasons (PK: year)
        season_values = [(year,) for year in seasons_to_upsert]
        if season_values:
            execute_values(cursor, "INSERT INTO seasons (year) VALUES %s ON CONFLICT (year) DO NOTHING;", season_values)
            logging.info(f"[DB] Upserted {len(seasons_to_upsert)} unique seasons.")

        # 2b. Venues (PK: venue_id)
        # Note: Added 'country' to ensure it's set on first insert
        venue_values = [tuple(v[col] for col in ['venue_id', 'name', 'city', 'country']) for v in venues_to_upsert.values()]
        if venue_values:
            venue_sql = """
                INSERT INTO venues (venue_id, name, city, country) 
                VALUES %s 
                ON CONFLICT (venue_id) DO UPDATE SET 
                    name = EXCLUDED.name, 
                    city = EXCLUDED.city,
                    country = EXCLUDED.country;
            """
            execute_values(cursor, venue_sql, venue_values)
            logging.info(f"[DB] Upserted {len(venues_to_upsert)} unique venues.")

        # 2c. Teams (PK: team_id) - Uses COALESCE to keep existing data if new data is null
        team_values = [
            (
                t.get('team_id'), 
                t.get('name'), 
                t.get('code'), # Will be None 
                t.get('country'), 
                t.get('founded'), # Will be None
                t.get('national', False), # Will be None, default to False if not provided during fixture sync
                t.get('logo_url'), 
                t.get('venue_id')
            ) 
            for t in teams_to_upsert.values()
        ]

        if team_values:
            # Columns in SQL: (team_id, name, code, country, founded, national, logo_url, venue_id) (8 columns)
            team_sql = """
                INSERT INTO teams (team_id, name, code, country, founded, national, logo_url, venue_id) 
                VALUES %s 
                ON CONFLICT (team_id) DO UPDATE SET 
                    name = COALESCE(EXCLUDED.name, teams.name),
                    code = COALESCE(EXCLUDED.code, teams.code),
                    country = COALESCE(EXCLUDED.country, teams.country), 
                    logo_url = COALESCE(EXCLUDED.logo_url, teams.logo_url),
                    -- ONLY update venue_id if the existing one is NULL or the new one is not NULL
                    venue_id = COALESCE(teams.venue_id, EXCLUDED.venue_id);
            """
            execute_values(cursor, team_sql, team_values)
            logging.info(f"[DB] Upserted {len(teams_to_upsert)} unique teams.")

        # 2d. Leagues (PK: league_id)
        league_values = [tuple(l[col] for col in ['league_id', 'name', 'type', 'logo_url', 'country_name']) for l in leagues_to_upsert.values()]
        if league_values:
            league_sql = """
                INSERT INTO leagues (league_id, name, type, logo_url, country_name) 
                VALUES %s 
                ON CONFLICT (league_id) DO UPDATE SET 
                    name = EXCLUDED.name, 
                    type = EXCLUDED.type, 
                    logo_url = EXCLUDED.logo_url, 
                    country_name = EXCLUDED.country_name;
            """
            execute_values(cursor, league_sql, league_values)
            logging.info(f"[DB] Upserted {len(leagues_to_upsert)} unique leagues.")
            
            # --- 2e. JIT UPSERT Enrichment Status (Set new leagues to PENDING/PRIORITY) ---
            thirty_days_ago = dt.datetime.now(tz=UTC) - dt.timedelta(days=30)
            enrichment_values = [(lid, 'PENDING' if lid not in PRIORITY_LEAGUE_IDS else 'PRIORITY', thirty_days_ago) for lid in leagues_to_upsert.keys()]
            if enrichment_values:
                enrichment_sql = """
                    INSERT INTO enrichment_status (league_id, status, last_enriched_at)
                    VALUES %s
                    ON CONFLICT (league_id) DO NOTHING;
                """
                execute_values(cursor, enrichment_sql, enrichment_values)
                
        # --- 3. UPSERT FIXTURES (in chunks) ---
        
        value_placeholders = ", ".join(UPSERT_COLUMNS) 
        upsert_sql = f"""
            INSERT INTO fixtures ({value_placeholders}) 
            VALUES %s
            ON CONFLICT (fixture_id) DO UPDATE SET
                referee = EXCLUDED.referee,
                date = EXCLUDED.date::TIMESTAMP WITH TIME ZONE, 
                "timestamp" = EXCLUDED.timestamp,
                status_long = EXCLUDED.status_long,
                status_short = EXCLUDED.status_short,
                elapsed = EXCLUDED.elapsed::INTEGER, 
                home_winner = EXCLUDED.home_winner::BOOLEAN,
                away_winner = EXCLUDED.away_winner::BOOLEAN,
                goals_home = EXCLUDED.goals_home::INTEGER,
                goals_away = EXCLUDED.goals_away::INTEGER,
                score_ht_home = EXCLUDED.score_ht_home::INTEGER,
                score_ht_away = EXCLUDED.score_ht_away::INTEGER,
                score_ft_home = EXCLUDED.score_ft_home::INTEGER,
                score_ft_away = EXCLUDED.score_ft_away::INTEGER,
                score_et_home = EXCLUDED.score_et_home::INTEGER, 
                score_et_away = EXCLUDED.score_et_away::INTEGER,
                score_pen_home = EXCLUDED.score_pen_home::INTEGER,
                score_pen_away = EXCLUDED.score_pen_away::INTEGER,
                
                -- Only update FKs if they were null (optional, safety first)
                league_id = COALESCE(fixtures.league_id, EXCLUDED.league_id),
                season_year = COALESCE(fixtures.season_year, EXCLUDED.season_year),
                home_team_id = COALESCE(fixtures.home_team_id, EXCLUDED.home_team_id),
                away_team_id = COALESCE(fixtures.away_team_id, EXCLUDED.away_team_id),
                venue_id = COALESCE(fixtures.venue_id, EXCLUDED.venue_id)
                
            RETURNING fixture_id, status_short;
        """
        total_upserted_count = 0
        
        for chunk in chunked(fixture_tuples, FIXTURE_UPSERT_CHUNK_SIZE):
            execute_values(cursor, upsert_sql, chunk)
            total_upserted_count += cursor.rowcount
            
            for row in cursor.fetchall():
                if row['status_short'] in ['TBD', 'NS', '1H', 'HT', '2H', 'ET', 'P', 'INT', 'FT']:
                    updated_fixture_ids.add(row['fixture_id'])

        conn.commit()
        logging.info(f"[DB] Successfully upserted {total_upserted_count} fixtures (across all chunks).")
        
    except Exception as e:
        conn.rollback()
        logging.error(f"[DB] Error during parent or initial upsert phase: {e}")
    finally:
        cursor.close()
        
    return updated_fixture_ids

async def worker_process_date(date_to_fetch: dt.date) -> Set[int]:
    """Async worker to fetch data for a date and update the DB."""
    date_str = date_to_fetch.isoformat()
    fixtures = []
    
    conn = db_utils.get_connection()
    if conn is None:
        return set()
        
    try:
        async with aiohttp.ClientSession(headers=API_HEADERS) as session:
            params = {"date": date_str}
            logging.info(f"[API] Fetching fixtures for date: {date_str}...")
            
            data = await async_get(session, AS_FIXTURES_URL, params)
            
            if data and data.get("response"):
                fixtures = data["response"]
                logging.info(f"[API] Received {len(fixtures)} fixtures for {date_str}.")
            
            if fixtures:
                # DB call remains sync/blocking, but it's executed after the async fetch
                return update_fixtures_db(fixtures, conn)
            
    finally:
        db_utils.release_connection(conn)
        
    return set()

# ============ LOW-FREQUENCY ENRICHMENT LOGIC (Teams & Standings) ============

async def fetch_and_upsert_teams(session, conn, league_id, season_year):
    """
    Async fetches all teams and their venues for a league, then sync updates the DB.
    """
    logging.info(f"[Enrichment] Fetching team details for League {league_id}, Season {season_year}.")
    
    params = {'league': league_id, 'season': season_year}
    
    try:
        data = await async_get(session, AS_TEAMS_URL, params)
        teams_data = data.get('response', []) if data else []
        
        if not teams_data:
            logging.warning(f"[Enrichment] No team data found for League {league_id}.")
            return 0
            
        # Use maps for deduplication
        team_data_map = {}  
        venue_data_map = {} 
        
        for item in teams_data:
            team = item.get('team', {})
            venue = item.get('venue', {})
            
            team_id = team.get('id')
            venue_id = venue.get('id')
            
            if team_id is not None and team_id not in team_data_map:
                # Prepare tuple for teams table (8 columns)
                team_data_map[team_id] = (
                    team_id, db_utils.safe_str(team.get('name')), db_utils.safe_str(team.get('code')),
                    db_utils.safe_str(team.get('country')), db_utils.safe_int(team.get('founded')),
                    team.get('national', False), db_utils.safe_str(team.get('logo')),
                    db_utils.safe_int(venue_id)
                )
            
            if venue_id is not None and venue_id not in venue_data_map:
                # Prepare tuple for venues table (7 columns)
                venue_data_map[venue_id] = (
                    venue_id, db_utils.safe_str(venue.get('name')), db_utils.safe_str(venue.get('address')),
                    db_utils.safe_str(venue.get('city')), db_utils.safe_int(venue.get('capacity')), 
                    db_utils.safe_str(venue.get('surface')), db_utils.safe_str(venue.get('image'))
                )
        
        team_tuples = list(team_data_map.values())
        venue_tuples = list(venue_data_map.values())

        # Upsert Venues (Synchronous DB call using provided conn)
        with conn.cursor() as cursor:
            venue_sql = """
                INSERT INTO venues (venue_id, name, address, city, capacity, surface, image_url)
                VALUES %s
                ON CONFLICT (venue_id) DO UPDATE SET 
                    name = COALESCE(EXCLUDED.name, venues.name), 
                    address = EXCLUDED.address,
                    city = EXCLUDED.city, 
                    capacity = EXCLUDED.capacity, 
                    surface = EXCLUDED.surface, 
                    image_url = EXCLUDED.image_url;
            """
            execute_values(cursor, venue_sql, venue_tuples)
            
            # Upsert Teams (Synchronous DB call using provided conn)
            team_sql = """
                INSERT INTO teams (team_id, name, code, country, founded, national, logo_url, venue_id) 
                VALUES %s 
                ON CONFLICT (team_id) DO UPDATE SET 
                    name = COALESCE(EXCLUDED.name, teams.name),
                    code = COALESCE(EXCLUDED.code, teams.code),
                    country = COALESCE(EXCLUDED.country, teams.country),
                    founded = COALESCE(EXCLUDED.founded, teams.founded),
                    national = EXCLUDED.national,
                    logo_url = COALESCE(EXCLUDED.logo_url, teams.logo_url),
                    venue_id = COALESCE(teams.venue_id, EXCLUDED.venue_id);
            """
            execute_values(cursor, team_sql, team_tuples)
            
        logging.info(f"[Enrichment] Successfully enriched {len(team_tuples)} unique teams for League {league_id}.")
        return 1
        
    except Exception as e:
        logging.error(f"[Enrichment] Failed to fetch/upsert teams for League {league_id}: {e}")
        return 0
        
async def fetch_and_upsert_standings(session, conn, league_id, season_year):
    """Async fetches standing data and sync updates the standings table."""
    logging.info(f"[Enrichment] Fetching standings for League {league_id}, Season {season_year}.")
    
    params = {'league': league_id, 'season': season_year}
    
    try:
        data = await async_get(session, AS_STANDINGS_URL, params)
        standings_response = data.get('response', []) if data else []
        
        if not standings_response:
            logging.warning(f"[Enrichment] No standings found for League {league_id}.")
            return 0
            
        # Flatten the list of standing groups/tables
        standings_lists = standings_response[0]['league']['standings']
        
        # Use a map for standings deduplication
        standings_data_map = {}
        
        for standings_list in standings_lists:
            for rank_data in standings_list:
                team_id = rank_data['team']['id']
                stats = rank_data.get('all', {})
                
                # Use a composite key for the map
                composite_key = (league_id, season_year, team_id)
                
                if composite_key not in standings_data_map:
                    # Prepare tuple for standings table (15 columns)
                    standings_data_map[composite_key] = (
                        league_id, 
                        season_year, 
                        team_id, 
                        rank_data.get('rank'),
                        rank_data.get('points'),
                        rank_data.get('goalsDiff'),
                        rank_data.get('group', 'N/A'),
                        rank_data.get('form'),
                        rank_data.get('description'),
                        stats.get('played'),
                        stats.get('win'),
                        stats.get('draw'),
                        stats.get('lose'),
                        stats.get('goals', {}).get('for'),
                        stats.get('goals', {}).get('against')
                    )

        standings_tuples = list(standings_data_map.values())
        
        # Upsert Standings (Synchronous DB call using provided conn)
        with conn.cursor() as cursor:
            standings_sql = """
                INSERT INTO standings (
                    league_id, season_year, team_id, "rank", points, goals_diff, 
                    group_name, form, description, played, win, draw, lose, 
                    goals_for, goals_against
                )
                VALUES %s
                ON CONFLICT (league_id, season_year, team_id) DO UPDATE SET
                    "rank" = EXCLUDED."rank",
                    points = EXCLUDED.points,
                    goals_diff = EXCLUDED.goals_diff,
                    group_name = EXCLUDED.group_name,
                    form = EXCLUDED.form,
                    description = EXCLUDED.description,
                    played = EXCLUDED.played,
                    win = EXCLUDED.win,
                    draw = EXCLUDED.draw,
                    lose = EXCLUDED.lose,
                    goals_for = EXCLUDED.goals_for,
                    goals_against = EXCLUDED.goals_against,
                    update_date = NOW();
            """
            execute_values(cursor, standings_sql, standings_tuples)

        logging.info(f"[Enrichment] Successfully upserted {len(standings_tuples)} standings entries for League {league_id}.")
        return 1
        
    except Exception as e:
        logging.error(f"[Enrichment] Failed to fetch/upsert standings for League {league_id}: {e}")
        return 0
        
async def run_enrichment_worker(league_id, season_year):
    """Executes all enrichment tasks for a single league using async calls."""
    conn = db_utils.get_connection()
    if conn is None:
        logging.error(f"[Enrichment] Failed to get DB connection for League {league_id}.")
        return False
        
    total_calls = 0
    try:
        async with aiohttp.ClientSession(headers=API_HEADERS) as session:
            # 1. Fetch & Upsert Teams/Venues (1 API call)
            total_calls += await fetch_and_upsert_teams(session, conn, league_id, season_year)
            
            # 2. Fetch & Upsert Standings (1 API call)
            if total_calls == 1:
                total_calls += await fetch_and_upsert_standings(session, conn, league_id, season_year)
            
        # 3. Mark as enriched and commit
        if total_calls == 2:
            with conn.cursor() as cursor:
                update_sql = "UPDATE enrichment_status SET status = 'ENRICHED', last_enriched_at = NOW() WHERE league_id = %s"
                cursor.execute(update_sql, (league_id,))
            conn.commit()
            logging.info(f"[Enrichment] League {league_id} marked as ENRICHED.")
            return True
        else:
            conn.rollback() # Rollback if either fetch failed
            return False
            
    except Exception as e:
        conn.rollback()
        logging.error(f"[Enrichment] Worker failed for League {league_id}: {e}")
        return False
    finally:
        db_utils.release_connection(conn)


async def run_enrichment_cycle():
    """
    The low-frequency manager for costly enrichment tasks, now using asyncio.gather.
    Enforces the 24-hour cool-down and 20-league batch limit.
    """
    global LAST_ENRICHMENT_RUN
    
    current_time = dt.datetime.now(tz=UTC)
    
    # 1. Check Global Cooldown for external leagues (non-priority)
    time_since_last_run = current_time - LAST_ENRICHMENT_RUN
    cooldown_delta = dt.timedelta(hours=COOLDOWN_HOURS)
    
    is_cooldown_active = time_since_last_run < cooldown_delta
    
    conn = db_utils.get_connection()
    if conn is None:
        return
        
    targets_to_run = []
    external_targets_count = 0
    
    try:
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        
        # --- 2. Determine Enrichment Targets (Sync DB calls) ---
        
        # a) PRIORITY Leagues (from mapping.json)
        priority_sql = f"""
            WITH latest_seasons AS (
                SELECT 
                    f.league_id, 
                    MAX(f.season_year) as season_year
                FROM fixtures f
                GROUP BY f.league_id
            )
            SELECT DISTINCT es.league_id, ls.season_year
            FROM enrichment_status es
            LEFT JOIN latest_seasons ls ON es.league_id = ls.league_id 
            WHERE es.status = 'PRIORITY' 
            AND ls.season_year IS NOT NULL
            AND (es.last_enriched_at < NOW() - INTERVAL '{COOLDOWN_HOURS} hours' OR es.last_enriched_at IS NULL);
        """
        cursor.execute(priority_sql)
        priority_targets = cursor.fetchall()
        targets_to_run.extend(priority_targets)
        
        # b) EXTERNAL (Non-Priority) Leagues - Only run if cooldown permits
        if not is_cooldown_active:
            external_sql = f"""
                SELECT DISTINCT es.league_id, ls.season_year
                FROM enrichment_status es
                JOIN league_seasons ls ON es.league_id = ls.league_id
                WHERE es.status = 'PENDING' AND ls.is_current = TRUE
                ORDER BY es.league_id ASC
                LIMIT {BATCH_SIZE // 2};
            """
            cursor.execute(external_sql)
            external_targets = cursor.fetchall()
            targets_to_run.extend(external_targets)
            external_targets_count = len(external_targets)
            
        if not targets_to_run:
            logging.info("[Enrichment] No pending leagues (PRIORITY or EXTERNAL) to enrich.")
            return

        logging.info(f"[Enrichment] Running enrichment on {len(targets_to_run)} leagues (Priority: {len(priority_targets)}, External: {external_targets_count}).")
        
    except Exception as e:
        conn.rollback()
        logging.error(f"[Enrichment] Error during target selection: {e}")
        return
    finally:
        db_utils.release_connection(conn)


    # --- 3. Execute Enrichment Tasks (Async Parallel) ---
    results = await asyncio.gather(
        *[run_enrichment_worker(t['league_id'], t['season_year']) for t in targets_to_run]
    )
    
    # --- 4. Update Cooldown Timer (After all async tasks finish) ---
    if external_targets_count > 0:
        # Only update the cooldown if we actually ran an external batch
        LAST_ENRICHMENT_RUN = current_time
        logging.info(f"[Enrichment] External enrichment batch complete. Cooldown reset to {COOLDOWN_HOURS} hours.")
        

def trigger_predictor(fixture_ids: Set[int]):
    """
    Executes predictor.py with the list of fixture IDs that need prediction.
    """
    if not fixture_ids:
        logging.info("No fixtures need prediction. Skipping predictor.py.")
        return
        
    logging.info(f"Triggering predictor.py for {len(fixture_ids)} fixtures...")
    
    # Convert set to comma-separated string for subprocess argument
    id_string = ",".join(map(str, fixture_ids))
    
    try:
        process = subprocess.run(
            [sys.executable, "predictor.py", "--fixtures", id_string],
            capture_output=True,
            text=True,
            check=True
        )
        # Only log success stdout to prevent excessive logs
        if process.returncode == 0:
            logging.info(f"PREDICTOR SUCCESS. STDOUT: {process.stdout.strip()}")
        else:
            raise subprocess.CalledProcessError(process.returncode, "predictor.py", output=process.stdout, stderr=process.stderr)

    except subprocess.CalledProcessError as e:
        logging.error(f"predictor.py failed with return code {e.returncode}")
        logging.error(f"PREDICTOR STDOUT: {e.stdout}")
        logging.error(f"PREDICTOR STDERR: {e.stderr}")
    except FileNotFoundError:
        logging.error("ERROR: predictor.py not found. Check file path.")
    except Exception as e:
        logging.error(f"ERROR executing predictor.py: {e}")

async def main_loop_async():
    """The main continuous polling loop using asyncio."""
    today_utc = dt.datetime.now(tz=UTC).date()
    yesterday_utc = today_utc - dt.timedelta(days=1)
    tomorrow_utc = today_utc + dt.timedelta(days=1)
    
    dates_to_sync = [yesterday_utc, today_utc, tomorrow_utc]
    
    # Initialize to force first check
    last_enrichment_check = dt.datetime.now(tz=UTC) - dt.timedelta(hours=10) 

    while True:
        cycle_start_time = time.time()
        
        # 1. Run High-Frequency Fixture Sync (Parallel using asyncio.gather)
        logging.info(f"\n--- Sync Cycle Starting for: {dates_to_sync[0].isoformat()} to {dates_to_sync[-1].isoformat()} ---")
        
        all_updated_ids: Set[int] = set()
        
        try:
            results = await asyncio.gather(
                *[worker_process_date(date) for date in dates_to_sync],
                return_exceptions=True
            )
            
            for result in results:
                if isinstance(result, Exception):
                    logging.error(f"[Async Worker] Exception during fixture sync: {result}")
                else:
                    all_updated_ids.update(result)
            
            logging.info(f"Total unique fixtures updated/checked for prediction: {len(all_updated_ids)}")
            
            # 2. Trigger Prediction on the relevant fixture IDs (Sync subprocess call)
            if all_updated_ids:
                # Running sync subprocess inside async is fine, but ensures it finishes before moving on
                trigger_predictor(all_updated_ids)

            # 3. Check and Run Low-Frequency Enrichment (Sequential async call)
            current_time = dt.datetime.now(tz=UTC)
            if (current_time - last_enrichment_check).total_seconds() >= ENRICHMENT_CHECK_INTERVAL_SECONDS:
                logging.info("[MainThread] Starting low-frequency enrichment check.")
                await run_enrichment_cycle()
                last_enrichment_check = current_time # Reset check timer

        except Exception as e:
            logging.error(f"[Sync] Critical error in main loop: {e}")
        
        cycle_end_time = time.time()
        elapsed = cycle_end_time - cycle_start_time
        
        sleep_duration = SYNC_INTERVAL_SECONDS - elapsed
        if sleep_duration < 0:
            sleep_duration = 0
            
        logging.info(f"Cycle finished in {elapsed:.2f}s. Sleeping for {sleep_duration:.2f}s...")
        await asyncio.sleep(sleep_duration)


def main():
    logging.info(f"--- Sync (Enricher) v4.14 Starting (Interval: {SYNC_INTERVAL_SECONDS / 60} min) ---")
    if not AS_API_KEY:
        logging.error("FATAL: AS_API_KEY not set. Sync script cannot run.")
        sys.exit(1)
        
    # 1. Load priority IDs from mapping file
    load_priority_league_ids()
        
    try:
        # 2. Initialize DB Connection Pool
        db_utils.init_connection_pool()
        
        # 3. CRITICAL: Initialize or confirm PRIORITY status for mapped leagues
        initialize_priority_status() 
        
        # 4. Start the main async sync loop
        asyncio.run(main_loop_async())
        
    except Exception as e:
        logging.critical(f"--- SYNC POLLER CRASHED: {e} ---")
    except KeyboardInterrupt:
        logging.info("--- SYNC POLLER STOPPING (KeyboardInterrupt) ---")
    finally:
        db_utils.close_all_connections()


if __name__ == "__main__":
    main()