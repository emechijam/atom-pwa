# csv_populator.py v3.4 (Batching Fix)
"""
THE TRANSPORTER (v3.4)

Mission:
1. Read all .csv files from the 'Data' folder.
2. Load the 'mapping.json' Rosetta Stone.
3. Translate names (e.g., 'E0', 'Arsenal') into IDs (e.g., 39, 42).
4. Populate the Supabase database.

V3.4 Changes:
- DB FIX (Batching): Re-architected the script to fix connection pool
  errors from "server closed the connection unexpectedly".
  - process_row() no longer touches the database. It only parses and
    returns a data dictionary.
  - The main thread now collects results into batches (DB_BATCH_SIZE).
  - A new function process_batch() upserts an entire batch in a
    single transaction.
  - This reduces 140,000+ individual transactions to ~140,
    which is efficient and pooler-friendly.
- DB FIX (Foreign Key): The new process_batch() function also
  bulk-upserts all seasons found in the batch (e.g., 2012) *before*
  inserting the fixtures.
  - This fixes the "fixtures_season_year_fkey" constraint violation.
- LOGGING: Updated log version to v3.4.
"""

import os
import csv
import sys
import logging
import datetime
import pytz
import re
import json
from concurrent.futures import ThreadPoolExecutor, as_completed
from psycopg2.errors import IntegrityError
from psycopg2.extras import execute_values
from dateutil import parser as date_parser

# Import database utilities
import db_utils 

# ============ CONFIG & LOGGING ============
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] [%(threadName)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

# --- Path to the Rosetta Stone ---
MAPPING_FILE = "mapping.json"
DATA_FOLDER = "Data"
MAX_WORKERS = 10  # For concurrent row processing (should match POOL_MAX)
DB_BATCH_SIZE = 1000 # Number of rows to insert per transaction

# --- Global Mappings ---
LEAGUE_MAP = {}
TEAM_MAP = {}

# ============ DATA MAPPING & TRANSLATION ============

def load_mappings():
    """Loads league and team mappings from mapping.json."""
    global LEAGUE_MAP, TEAM_MAP
    try:
        with open(MAPPING_FILE, 'r', encoding='utf-8') as f:
            mappings = json.load(f)
            LEAGUE_MAP = mappings.get("leagues", {})
            TEAM_MAP = mappings.get("teams", {})
        
        if not LEAGUE_MAP or not TEAM_MAP:
            logging.error("mapping.json is missing 'leagues' or 'teams' keys.")
            sys.exit(1)
            
        logging.info(f"Successfully loaded {len(LEAGUE_MAP)} league and {len(TEAM_MAP)} team mappings.")
        
    except FileNotFoundError:
        logging.error(f"CRITICAL: {MAPPING_FILE} not found.")
        sys.exit(1)
    except json.JSONDecodeError:
        logging.error(f"CRITICAL: Could not parse {MAPPING_FILE}. Check for syntax errors.")
        sys.exit(1)

def get_league_id(fd_code):
    """Translates a football-data code (e.g., 'E0') to an API-Football ID."""
    league_info = LEAGUE_MAP.get(fd_code)
    if league_info:
        return league_info.get("api_football_id")
    logging.warning(f"No API-Football ID found for league code '{fd_code}' in mapping.json.")
    return None

def get_team_id(fd_name):
    """Translates a football-data team name (e.g., 'Arsenal') to an API-Football ID."""
    team_info = TEAM_MAP.get(fd_name)
    if team_info:
        return team_info.get("api_football_id")
    logging.warning(f"No API-Football ID found for team name '{fd_name}' in mapping.json.")
    return None

def parse_season_from_filename(filename):
    """
    Parses a season start year (e.g., 2023) from a filename
    (e.g., "England 2023 2024 Premier League.csv").
    """
    # Regex to find a 4-digit year (e.g., 2023) followed by another 4-digit year
    match = re.search(r'(\d{4})\s*(\d{4})', filename)
    if match:
        return int(match.group(1))
    return None

def get_season_from_date(date_obj):
    """
    Calculates the 'season_start_year' from a match date.
    e.g., Aug 2023 -> 2023 season. Feb 2024 -> 2023 season.
    """
    if date_obj.month >= 7:  # Season starts (e.g., Aug 2023 -> 2023)
        return date_obj.year
    else:  # Season is finishing (e.g., Feb 2024 -> 2023)
        return date_obj.year - 1

def parse_date(date_str, time_str=""):
    """
    Parses date and time strings into a timezone-aware datetime object.
    Tries multiple formats for robustness.
    """
    if not date_str:
        return None

    try:
        # Let dateutil.parser handle various formats (e.g., dd/mm/yy, dd/mm/yyyy)
        # dayfirst=True is crucial for UK/Euro data formats
        parsed_dt = date_parser.parse(date_str, dayfirst=True)
        
        if time_str:
            try:
                parsed_time = date_parser.parse(time_str).time()
                parsed_dt = parsed_dt.replace(hour=parsed_time.hour, minute=parsed_time.minute)
            except Exception:
                logging.warning(f"Could not parse time '{time_str}', using 00:00.")
                
        # Assume all times are local to London/Europe and convert to UTC
        local_tz = pytz.timezone("Europe/London")
        aware_dt = local_tz.localize(parsed_dt)
        return aware_dt.astimezone(pytz.utc)
        
    except Exception:
        logging.warning(f"Could not parse date: '{date_str} {time_str}'. Skipping row.")
        return None

def to_int(value):
    """Safely converts a value to an integer, returning None on failure."""
    try:
        return int(float(value))
    except (ValueError, TypeError):
        return None

def get_fd_code_from_filename(filename):
    """
    Parses complex filenames to find the correct football-data.co.uk league code.
    e.g., "England 2023 2024 Premier League.csv" -> "E0"
    """
    filename_lower = filename.lower()
    
    # --- Multi-League Countries (Must check first) ---

    # England
    if 'england' in filename_lower:
        if 'premier league' in filename_lower:
            return 'E0'
        if 'championship' in filename_lower:
            return 'E1'
        if 'league 1' in filename_lower:
            return 'E2'
        if 'league 2' in filename_lower:
            return 'E3'
        if 'national league' in filename_lower:
            return 'EC'
            
    # Germany
    if 'germany' in filename_lower:
        if 'bundesligas 1' in filename_lower:
            return 'D1'
        if 'bundesligas 2' in filename_lower:
            return 'D2'

    # Spain
    if 'spain' in filename_lower:
        if 'premera' in filename_lower:
            return 'SP1'
        if 'segunda' in filename_lower:
            return 'SP2'
            
    # Italy
    if 'italy' in filename_lower:
        if 'serie a' in filename_lower:
            return 'I1'
        if 'serie b' in filename_lower:
            return 'I2'

    # France
    if 'france' in filename_lower:
        if 'le championnat' in filename_lower:
            return 'F1'
        if 'division 2' in filename_lower:
            return 'F2'
            
    # Scotland
    if 'scotland' in filename_lower:
        if 'premiership' in filename_lower:
            return 'SC0'
        if 'divsion 1' in filename_lower: # Typo "Divsion" is in your filenames
            return 'SC1'
        if 'divsion 2' in filename_lower:
            return 'SC2'
        if 'divsion 3' in filename_lower:
            return 'SC3'

    # --- Single-League Countries ---

    if 'argentina' in filename_lower:
        return 'ARG'
    if 'austria' in filename_lower:
        return 'AUT'
    if 'belgium' in filename_lower:
        return 'B1' # All Belgium files are Jupiler League
    if 'brazil' in filename_lower:
        return 'BRA'
    if 'china' in filename_lower:
        return 'CHN'
    if 'denmark' in filename_lower:
        return 'DNK'
    if 'finland' in filename_lower:
        return 'FIN'
    if 'greece' in filename_lower:
        return 'G1'
    if 'ireland' in filename_lower:
        return 'IRL'
    if 'japan' in filename_lower:
        return 'JPN'
    if 'mexico' in filename_lower:
        return 'MEX'
    if 'netherlands' in filename_lower:
        return 'N1' # All seem to be Eredivisie
    if 'norway' in filename_lower:
        return 'NOR'
    if 'poland' in filename_lower:
        return 'POL'
    if 'portugal' in filename_lower:
        return 'P1' # All seem to be Liga I
    if 'romania' in filename_lower:
        return 'ROU'
    if 'russia' in filename_lower:
        return 'RUS'
    if 'sweden' in filename_lower:
        return 'SWE'
    if 'switzerland' in filename_lower:
        return 'SWZ'
    if 'turkey' in filename_lower:
        return 'T1'
    if 'usa' in filename_lower:
        return 'USA'

    return None # No match

def generate_fixture_id(league_id, home_team_id, away_team_id, season_year, date_obj):
    """
    Creates a stable, unique, NEGATIVE fixture_id based on hashed contents.
    We add the date string to differentiate potential duplicate matches in
    the same season (e.g. cup + league).
    """
    # Create a stable string
    date_str = date_obj.strftime('%Y-%m-%d')
    id_string = f"fd:{league_id}:{season_year}:{home_team_id}:{away_team_id}:{date_str}"
    # Hash the string and ensure it's a negative integer
    return -abs(hash(id_string) % (10**9)) # Modulo to keep it within integer range

# ============ DATABASE POPULATION (THE TRANSPORT) ============

def upsert_country(cursor, name, code=None, flag=None):
    """Inserts or updates a country."""
    # FIX v3.3: The schema column name is 'flag_url', not 'flag'.
    sql = """
    INSERT INTO countries (name, code, flag_url)
    VALUES (%s, %s, %s)
    ON CONFLICT (name) DO NOTHING;
    """
    cursor.execute(sql, (name, code, flag))

def upsert_league(cursor, league_id, name, type, logo, country_name):
    """Inserts or updates a league, linking it to its country."""
    sql = """
    INSERT INTO leagues (league_id, name, type, logo_url, country_name)
    VALUES (%s, %s, %s, %s, %s)
    ON CONFLICT (league_id) DO UPDATE SET
        name = EXCLUDED.name,
        type = EXCLUDED.type,
        logo_url = EXCLUDED.logo_url,
        country_name = EXCLUDED.country_name;
    """
    cursor.execute(sql, (league_id, name, type, logo, country_name))

def upsert_team(cursor, team_id, name, country, logo):
    """Inserts or updates a team."""
    sql = """
    INSERT INTO teams (team_id, name, country, logo_url)
    VALUES (%s, %s, %s, %s)
    ON CONFLICT (team_id) DO UPDATE SET
        name = EXCLUDED.name,
        country = EXCLUDED.country,
        logo_url = EXCLUDED.logo_url;
    """
    # NOTE: This assumes 'logo_url' is the column name in 'teams' table.
    # From create_api_football_schema.sql:
    # CREATE TABLE IF NOT EXISTS teams ( ... logo_url TEXT, ... )
    # This is correct.
    cursor.execute(sql, (team_id, name, country, logo))

def upsert_season(cursor, year):
    """Inserts or updates a season."""
    sql = """
    INSERT INTO seasons (year)
    VALUES (%s)
    ON CONFLICT (year) DO NOTHING;
    """
    # NOTE: The schema has 'year' as the PRIMARY KEY.
    # The 'populator.py' script was trying to insert start/end dates
    # that don't exist in the 'csv_populator' version of this function.
    # This version is correct for the schema.
    cursor.execute(sql, (year,))

def upsert_fixture_batch(cursor, fixtures_data):
    """
    Inserts or updates a batch of fixtures using execute_values.
    This is much faster than one-by-one inserts.
    """
    sql = """
    INSERT INTO fixtures (
        fixture_id, league_id, season_year, date, 
        home_team_id, away_team_id, 
        goals_home, goals_away,
        status_short, status_long
    ) 
    VALUES %s
    ON CONFLICT (fixture_id) DO UPDATE SET
        date = EXCLUDED.date,
        goals_home = EXCLUDED.goals_home,
        goals_away = EXCLUDED.goals_away,
        status_short = EXCLUDED.status_short,
        status_long = 'Match Finished';
    """
    
    # Transform the list of dicts into a list of tuples
    values_list = [
        (
            data['fixture_id'], data['league_id'], data['season_year'], data['date'],
            data['home_team_id'], data['away_team_id'],
            data['goals_home'], data['goals_away'],
            'FT', 'Match Finished'
        )
        for data in fixtures_data
    ]
    
    if not values_list:
        return

    try:
        execute_values(cursor, sql, values_list)
    except Exception as e:
        logging.error(f"Error in bulk upsert: {e}")
        raise # Re-raise to trigger rollback

def process_batch(fixtures_list, seasons_set):
    """
    Gets a connection and processes a batch of fixtures and seasons
    in a single transaction.
    """
    conn = None
    try:
        conn = db_utils.get_connection()
        if conn is None:
            logging.error("Could not get connection from pool for batch worker.")
            return

        with conn.cursor() as cursor:
            # 1. Upsert Seasons FIRST (Fixes Foreign Key Error)
            # Make sure all seasons (e.g., 2012) exist before adding fixtures
            if seasons_set:
                season_values = [(year,) for year in seasons_set]
                execute_values(cursor, "INSERT INTO seasons (year) VALUES %s ON CONFLICT (year) DO NOTHING;", season_values)

            # 2. Bulk Upsert Fixtures (Fixes Connection Pool Error)
            upsert_fixture_batch(cursor, fixtures_list)
        
        conn.commit()
        logging.info(f"Successfully processed batch of {len(fixtures_list)} fixtures.")
        
    except Exception as e:
        if conn:
            conn.rollback()
        logging.error(f"Failed to process batch: {e}", exc_info=True)
    finally:
        if conn:
            db_utils.release_connection(conn)


def process_row(row, fd_league_code, season_year_from_filename):
    """
    Processes a single CSV row.
    This function is run by worker threads.
    It no longer touches the database; it just parses and returns data.
    """
    try:
        # 1. Parse Date (Needed for season calculation)
        date_str = row.get("Date", "")
        time_str = row.get("Time", "")
        date_obj = parse_date(date_str, time_str)
        
        if not date_obj:
            logging.warning(f"Skipping row, could not parse date: {row}")
            return None

        # 2. Determine Season
        season_year = season_year_from_filename
        if not season_year:
            season_year = get_season_from_date(date_obj)
        
        if not season_year:
             logging.warning(f"Skipping row, could not determine season: {row}")
             return None
             
        # 3. Translate League
        league_id = get_league_id(fd_league_code)
        if not league_id:
            logging.warning(f"Skipping row, unknown league code: {fd_league_code}")
            return None
        
        # 4. Translate Teams
        # FIX v3.3: Try 'HomeTeam' first (standard), fallback to 'Home' (alternate)
        home_team_name = row.get("HomeTeam") or row.get("Home")
        away_team_name = row.get("AwayTeam") or row.get("Away")
        
        if not home_team_name or not away_team_name:
            logging.warning(f"Skipping row, missing HomeTeam/Home or AwayTeam/Away. Keys: {list(row.keys())}")
            return None
            
        home_team_id = get_team_id(home_team_name)
        away_team_id = get_team_id(away_team_name)
        
        if not home_team_id or not away_team_id:
            logging.warning(f"Skipping row, unknown team: {home_team_name} or {away_team_name}")
            return None

        # 5. Generate Fixture ID
        fixture_id = generate_fixture_id(league_id, home_team_id, away_team_id, season_year, date_obj)

        # 6. Prepare Data
        data = {
            "fixture_id": fixture_id,
            "league_id": league_id,
            "season_year": season_year,
            "date": date_obj,
            "home_team_id": home_team_id,
            "away_team_id": away_team_id,
            "goals_home": to_int(row.get("FTHG")), # Full Time Home Goals
            "goals_away": to_int(row.get("FTAG")), # Full Time Away Goals
        }
        
        # Handle alternate goal column names (e.g., 'HG', 'AG')
        if data["goals_home"] is None:
             data["goals_home"] = to_int(row.get("HG"))
        if data["goals_away"] is None:
             data["goals_away"] = to_int(row.get("AG"))
        
        # 7. Return data for batching
        return data

    except Exception as e:
        # Log other errors with more detail
        logging.error(f"Failed to process row: {e}. Row: {row}", exc_info=True)
        return None


def populate_lookup_tables():
    """
    Populates countries, leagues, and teams from the mapping file.
    Populates seasons from the CSV filenames.
    """
    logging.info("Populating lookup tables (countries, leagues, teams, seasons)...")
    conn = None
    try:
        conn = db_utils.get_connection()
        if not conn:
            logging.error("Failed to get connection for lookup table population.")
            return

        with conn.cursor() as cursor:
            # 1. Populate Countries & Leagues
            for fd_code, league_info in LEAGUE_MAP.items():
                api_id = league_info.get("api_football_id")
                country_name = league_info.get("country")
                
                if not api_id or not country_name:
                    logging.warning(f"Skipping league {fd_code}, missing 'api_football_id' or 'country'.")
                    continue
                
                # Upsert Country first
                upsert_country(cursor, country_name)
                
                # Upsert League
                upsert_league(
                    cursor,
                    api_id,
                    league_info.get("api_football_name", "Unknown League"), # Use 'api_football_name'
                    "League", # Default type
                    None, # Logo
                    country_name
                )

            # 2. Populate Teams
            for fd_name, team_info in TEAM_MAP.items():
                api_id = team_info.get("api_football_id")
                if not api_id:
                    logging.warning(f"Skipping team {fd_name}, missing 'api_football_id'.")
                    continue
                
                upsert_team(
                    cursor,
                    api_id,
                    fd_name, # Use the name from the mapping key
                    team_info.get("country", "Unknown"),
                    None # Logo
                )
            
            # 3. Populate Seasons (from filenames)
            all_seasons = set()
            for original_filename in os.listdir(DATA_FOLDER):
                filename = original_filename
                if filename.endswith('.csv.csv'):
                    filename = filename[:-4] # Clean double extension
                    
                season = parse_season_from_filename(filename)
                if season:
                    all_seasons.add(season)
            
            # This is where 2012 was missed. It will be
            # caught by the new process_batch() function.
            for year in all_seasons:
                upsert_season(cursor, year)

        conn.commit()
        logging.info("Lookup tables populated successfully.")
        
    except Exception as e:
        if conn:
            conn.rollback()
        logging.error(f"Error populating lookup tables: {e}", exc_info=True)
    finally:
        if conn:
            db_utils.release_connection(conn)

def main():
    logging.info("--- CSV Populator (Transporter) v3.4 Starting ---")
    
    try:
        load_mappings()
        db_utils.init_connection_pool()
        populate_lookup_tables()
    except Exception as e:
        logging.critical(f"Startup failed: {e}", exc_info=True)
        return

    logging.info(f"Starting fixture processing from '{DATA_FOLDER}'...")
    
    total_rows_submitted = 0
    all_fixtures_to_upsert = []
    all_seasons_to_upsert = set()
    
    with ThreadPoolExecutor(max_workers=MAX_WORKERS, thread_name_prefix="RowWorker") as executor:
        futures = []
        
        for original_filename in os.listdir(DATA_FOLDER):
            filename = original_filename
            
            # Handle double extensions like '...Bundesligas 1.csv.csv'
            if filename.endswith('.csv.csv'):
                 filename = filename[:-4]
            
            if not filename.endswith('.csv'):
                continue
                
            file_path = os.path.join(DATA_FOLDER, original_filename)
            logging.info(f"Processing file: {original_filename}")
            
            # Determine league code and season from file
            fd_league_code = get_fd_code_from_filename(filename)
            season_year = parse_season_from_filename(filename) # This can be None
            
            if not fd_league_code:
                logging.warning(f"Skipping file: Could not determine league code from filename '{original_filename}'")
                continue

            if not get_league_id(fd_league_code):
                logging.warning(f"Skipping file: No league mapping for code '{fd_league_code}' from filename '{original_filename}'")
                continue
                
            # We no longer skip if season_year is None, process_row will handle it
                
            try:
                with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
                    # Handle potential UTF-8 BOM at the start of the file
                    first_line = f.readline()
                    if '\ufeff' in first_line:
                        # Reset and read again with correct fieldnames
                        f.seek(0)
                        reader = csv.DictReader(f)
                        # Clean up fieldnames that might contain the BOM
                        reader.fieldnames = [name.lstrip('\ufeff') for name in reader.fieldnames]
                    else:
                        # Reset and read normally
                        f.seek(0)
                        reader = csv.DictReader(f)

                    rows_in_file = 0
                    for row in reader:
                        futures.append(executor.submit(
                            process_row,
                            row,
                            fd_league_code,
                            season_year # Pass None if not found
                        ))
                        rows_in_file += 1
                    total_rows_submitted += rows_in_file
                    
            except Exception as e:
                logging.error(f"Failed to read file {original_filename}: {e}")

        # Wait for all rows to be processed
        logging.info(f"Waiting for {total_rows_submitted} total rows to be processed...")
        
        for future in as_completed(futures):
            try:
                result = future.result()  # Get the data dict (or None)
                if result:
                    all_fixtures_to_upsert.append(result)
                    all_seasons_to_upsert.add(result['season_year'])
                
                # Check if the batch is full
                if len(all_fixtures_to_upsert) >= DB_BATCH_SIZE:
                    logging.info(f"Batch size {DB_BATCH_SIZE} reached. Processing batch...")
                    process_batch(all_fixtures_to_upsert, all_seasons_to_upsert)
                    # Clear the lists for the next batch
                    all_fixtures_to_upsert = []
                    all_seasons_to_upsert = set()
                    
            except Exception as e:
                logging.error(f"A row worker failed unexpectedly: {e}", exc_info=True)
    
    # Process any remaining fixtures that didn't fill a whole batch
    if all_fixtures_to_upsert:
        logging.info(f"Processing final batch of {len(all_fixtures_to_upsert)} fixtures...")
        process_batch(all_fixtures_to_upsert, all_seasons_to_upsert)

    logging.info("--- CSV Populator Finished ---")
    db_utils.close_all_connections()


if __name__ == "__main__":
    main()