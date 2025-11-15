#!/usr/bin/env python3
"""
Supabase CSV Converter Script (v5.7)
This script converts "offline" CSV data from two sources:
1.  'fd_historical_data_2022-2025.csv' (from football-data.org)
2.  The 'Data/' folder (from football-data.co.uk)
It transforms this data into a set of new CSV files that perfectly match the
updated Supabase database schema.

WHAT'S NEW (v5.7):
- FIX: Refactored FD_UK folder processing to use standard csv.DictReader on the file stream,
  resolving potential errors and skipped rows caused by non-standard header searching logic.
- IMPROVEMENT: Enhanced robustness of date parsing for two-digit years in FD_UK files.

WHAT'S NEW (v5.6):
- FIX: Updated safe_int to handle empty strings ('') silently, resolving the massive
  spam of 'ValueError: invalid literal for int() with base 10: ''' warnings.
- FIX: Updated safe_int to use `default=0` consistently for score fields.

WHAT'S NEW (v5.5):
- ADDED COLUMN MAPPING: For files with non-standard column names (e.g., 'Home' instead of 'HomeTeam', 'HG' instead of 'FTHG' in USA MLS), map them to standard names.
- FIXED MLS PROCESSING: Now processes USA Football MLS.csv correctly without skipping all rows.

WHAT'S NEW (v5.4):
- ADDED BOM HANDLING: Use encoding='utf-8-sig' to handle BOM in CSV files.
- ADDED KEY CLEANING: Strip BOM and whitespace from row keys to fix KeyError issues.
- IMPROVED REQUIRED FIELDS CHECK: Log missing fields more clearly.

WHAT'S NEW (v5.3):
- REMOVED MATCHES COLUMN: Removed the 'matches' column from teams to match the provided schema.
- ADDED IMPORT ORDER NOTE: Prints the recommended import order for Supabase to avoid foreign key errors.
- ADDED ROW CLEANING: Cleans bad keys from rows and skips rows missing required fields to handle KeyError 'unknown'.

WHAT'S NEW (v5.2):
- ADDED SAFE_INT: Added a safe_int function to handle invalid values in score fields (e.g., 'unknown') by logging and using a default value, preventing skips due to ValueError.
- DETAILED ERROR LOGGING: Updated error logging to include exception type and message for better debugging (e.g., to identify if 'unknown' is KeyError or ValueError).

WHAT'S NEW (v5.1):
- ADDED ROBUST ROW PROCESSING: Added try-except blocks inside the row processing loops to skip bad rows and log errors, preventing the script from stopping on invalid data.

WHAT'S NEW (v5.0):
- UPDATED TO NEW SCHEMA: Adjusted to match the provided PostgreSQL schema.
  - Countries replace areas (name as PK).
  - Seasons are now just unique years.
  - League_seasons for league-season details.
  - Teams use negative integer IDs as placeholders (to avoid API conflicts).
  - Fixtures use negative integer IDs, compute winners, total goals, status, etc.
  - Referees are now just TEXT in fixtures (no separate table).
  - Omitted generated columns like league_seasons.id and fixtures.created_at.
- COMPUTED FIELDS: Winners, total goals (after ET), status_short/long, elapsed, timestamp.
- PLACEHOLDER IDs: Negative integers for teams and fixtures.

ONLINE OPERATION:
- Fetches leagues and seasons from API-Football for master lookups.

OFFLINE OPERATION:
- Processes input CSVs and maps to new schema.
- Generates placeholder integers for missing API IDs.

OUTPUT:
- A new folder named 'supabase_upload' containing:
    - countries.csv
    - leagues.csv
    - seasons.csv
    - league_seasons.csv
    - teams.csv
    - fixtures.csv
"""
import os
import csv
import logging
import requests
import re
import uuid 
import json 
import pytz
import io
from pathlib import Path
from datetime import datetime
from typing import Dict, Any, Optional, Set, List

# ============ CONFIGURATION ============
# --- API-Football (Online Step) ---
API_FOOTBALL_KEY = "5c447790790568e2c4178ef898da698e"
API_LEAGUES_URL = "https://v3.football.api-sports.io/leagues"
# --- Input Files (Offline Data) ---
FD_API_CSV = "fd_historical_data_2022-2025.csv"
FD_UK_FOLDER = "Data"
# --- Output Folder ---
OUTPUT_FOLDER = "supabase_upload"
# --- Logging Setup ---
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
# ============ IN-MEMORY DATABASE ============
# These dictionaries will store our new, clean data.
COUNTRIES_DB: Dict[str, Dict[str, Any]] = {}  # key: normalized_name
LEAGUES_DB: Dict[int, Dict[str, Any]] = {}  # key: league_id (API integer)
SEASONS_SET: Set[int] = set()  # Unique years
LEAGUE_SEASONS_DB: Dict[str, Dict[str, Any]] = {}  # key: "league_id_year" (no id, as generated)
TEAMS_DB: Dict[str, Dict[str, Any]] = {}  # key: normalized_name
FIXTURES_DB: Dict[str, Dict[str, Any]] = {}  # key: unique_fixture_key

# Counters for placeholder IDs (negative to avoid API conflicts)
TEAM_ID_COUNTER = -1
FIXTURE_ID_COUNTER = -1

# Master lookup for matching CSV league names to real API-Football IDs
# key: normalized "league_name|country_name"
# value: api_league_id
LEAGUE_NAME_MAP: Dict[str, int] = {}

# ============ HELPER FUNCTIONS ============
def normalize_name(name: Optional[str]) -> str:
    """Cleans a name for consistent lookups."""
    if not name:
        return "unknown"
    return re.sub(r"[^a-z0-9]", "", name.lower().strip())

def safe_int(value: Any, default: Optional[int] = 0) -> Optional[int]:
    """Safely converts value to int, handling invalid cases and empty strings."""
    if value is None:
        return default
    
    str_value = str(value).strip().lower()
    
    # If the stripped value is an empty string or 'unknown', return default quietly.
    if not str_value or str_value == 'unknown':
        return default
        
    try:
        # Use the cleaned string value for conversion
        return int(str_value)
    except (ValueError, TypeError) as e:
        # Only log warning for non-standard failure cases (e.g., '12.5', 'abc')
        logging.warning(f"Invalid int value: {value}, using default {default}. Error: {type(e).__name__}: {e}")
        return default

def get_or_create_country(name: str, code: Optional[str] = None, flag_url: Optional[str] = None) -> str:
    """
    Finds or creates a Country.
    Returns the name (PK).
    """
    if not name:
        name = "Unknown"
    norm_name = normalize_name(name)
    if norm_name not in COUNTRIES_DB:
        COUNTRIES_DB[norm_name] = {
            "name": name,
            "code": code,
            "flag_url": flag_url,
        }
    return COUNTRIES_DB[norm_name]["name"]

def get_or_create_team(name: str) -> int:
    """Finds or creates a Team. Assigns a placeholder negative integer ID."""
    global TEAM_ID_COUNTER
    if not name:
        name = "Unknown Team"
    norm_name = normalize_name(name)
    if norm_name not in TEAMS_DB:
        TEAM_ID_COUNTER -= 1
        TEAMS_DB[norm_name] = {
            "team_id": TEAM_ID_COUNTER,
            "name": name,
            "code": None,
            "country": None,
            "founded": None,
            "national": False,
            "logo_url": None,
            "venue_id": None,
        }
    return TEAMS_DB[norm_name]["team_id"]

def get_or_create_league_season(league_id: int, year: int, start_date: str, end_date: str, is_current: bool = False) -> None:
    """Creates a League_Season if not exists."""
    season_key = f"{league_id}_{year}"
    if season_key not in LEAGUE_SEASONS_DB:
        LEAGUE_SEASONS_DB[season_key] = {
            "league_id": league_id,
            "season_year": year,
            "start_date": start_date,
            "end_date": end_date,
            "is_current": is_current,
        }
    SEASONS_SET.add(year)

def parse_fd_uk_date(date_str: str) -> Optional[datetime]:
    """
    Parses 'dd/mm/yy' or 'dd/mm/YYYY' formats.
    Handles 2-digit years.
    """
    if not date_str:
        return None
    
    date_str = date_str.strip()
    
    # Try 4-digit year format first
    try:
        return datetime.strptime(date_str, "%d/%m/%Y")
    except ValueError:
        pass

    # Try 2-digit year format. Python's strptime handles the century cutoff
    try:
        dt = datetime.strptime(date_str, "%d/%m/%y")
        # Add a check to prevent parsing 70s as 2070s in current data range
        if dt.year > datetime.now().year + 5:
             # e.g., if current year is 2025, and %y parsed '70' as 2070, we correct it to 1970
            dt = dt.replace(year=dt.year - 100) 
        return dt
    except ValueError:
        logging.warning(f"Could not parse date: {date_str}")
        return None

def parse_fd_api_date(date_str: str) -> Optional[datetime]:
    """Parses ISO date with 'T' and 'Z'."""
    if not date_str:
        return None
    try:
        return datetime.fromisoformat(date_str.replace("Z", "+00:00"))
    except ValueError:
        try:
            return datetime.strptime(date_str, "%Y-%m-%d %H:%M:%S").replace(tzinfo=pytz.UTC)
        except ValueError:
            logging.warning(f"Could not parse ISO date: {date_str}")
            return None

# ============ STEP 1: ONLINE - FETCH LEAGUES ============
def fetch_api_football_leagues():
    """
    Fetches all league and season data from API-Football.
    This is the only online step.
    Populates: LEAGUES_DB, COUNTRIES_DB, LEAGUE_SEASONS_DB, SEASONS_SET, and LEAGUE_NAME_MAP
    """
    logging.info("--- STEP 1: Fetching Master League List from API-Football ---")
    headers = {
        "x-apisports-key": API_FOOTBALL_KEY
    }
    try:
        response = requests.get(API_LEAGUES_URL, headers=headers, timeout=30)
        response.raise_for_status()
        data = response.json()
        if not data.get("response"):
            logging.error("No 'response' in API data. Check API key.")
            return
        leagues_fetched = 0
        for item in data["response"]:
            league = item.get("league")
            country = item.get("country")
            seasons = item.get("seasons")
            if not all([league, country, seasons]):
                continue
            # 1. Create the Country
            country_name = get_or_create_country(
                country.get("name"),
                country.get("code"),
                country.get("flag")
            )
            # 2. Create the League
            league_id = league.get("id")
            if league_id not in LEAGUES_DB:
                LEAGUES_DB[league_id] = {
                    "league_id": league_id,
                    "name": league.get("name"),
                    "type": league.get("type"),
                    "logo_url": league.get("logo"),
                    "country_name": country_name,
                }
                leagues_fetched += 1
            # 3. Create the League_Seasons and Seasons
            for season in seasons:
                year = season.get("year")
                if not year:
                    continue
                start_date = season.get("start") or f"{year}-01-01"
                end_date = season.get("end") or f"{year}-12-31"
                is_current = season.get("current", False)
                get_or_create_league_season(league_id, year, start_date, end_date, is_current)
            # 4. Create the Lookup Map
            map_key = f"{normalize_name(league.get('name'))}|{normalize_name(country.get('name'))}"
            LEAGUE_NAME_MAP[map_key] = league_id
        logging.info(f"Successfully fetched and stored {leagues_fetched} leagues.")
        logging.info(f"Built master lookup map with {len(LEAGUE_NAME_MAP)} entries.")
    except requests.exceptions.RequestException as e:
        logging.error(f"API-Football request FAILED: {type(e).__name__}: {e}")
        logging.error("Continuing to process CSVs, but league lookups will fail without fresh API data.")
    except json.JSONDecodeError as e:
        logging.error(f"Failed to decode JSON from API-Football: {type(e).__name__}: {e}")

# ============ STEP 2: OFFLINE - MANUAL MAPPINGS ============
LEAGUE_NAME_ALIAS_MAP = {
    # FD_API_CSV ('LeagueCode')
    "PL": "Premier League",
    "CL": "UEFA Champions League",
    "BL1": "Bundesliga",
    "SA": "Serie A",
    "FL1": "Ligue 1",
    "PPL": "Primeira Liga",
    "EC": "Euro Championship",
    "WC": "World Cup",
    "ELC": "Championship",
    "SP1": "La Liga",
    # FD_UK_FOLDER (from filenames)
    "Premier League": "Premier League",
    "EFL Championship": "Championship",
    "EFL League 1": "League One",
    "EFL League 2": "League Two",
    "National League": "National League",
    "Premiership": "Premiership",
    "Divsion 1": "Championship", # Scotland (SC1)
    "Divsion 2": "League One", # Scotland (SC2)
    "Divsion 3": "League Two", # Scotland (SC3)
    "Bundesligas 1": "Bundesliga",
    "Bundesligas 2": "2. Bundesliga",
    "Serie B": "Serie B",
    "La Liga Premera": "La Liga",
    "La Liga Segunda": "Segunda División",
    "Le Championnat": "Ligue 1",
    "Division 2": "Ligue 2",
    "KPN Eredivisie": "Eredivisie",
    "Liga I": "Liga Portugal", # Portugal
    "Jupiler League": "Jupiler Pro League",
    "Ethniki Katigoria": "Super League 1",
    "Ligi 1": "Süper Lig",
    "Bundesliga": "Bundesliga", # Austria
    "Primera Division": "Liga Profesional Argentina",
    "Serie A": "Serie A", # Brazil
    "Super League": "Super League", # China, Switzerland, Greece
    "Superliga": "Superliga", # Denmark
    "Veikkausliiga": "Veikkausliiga",
    "Premier Division": "Premier Division", # Ireland
    "J-League": "J1 League",
    "Liga MX": "Liga MX",
    "Eliteserien": "Eliteserien",
    "Ekstraklasa": "Ekstraklasa",
    "Liga 1": "Liga I", # Romania
    "Allsvenskan": "Allsvenskan",
    "MLS": "Major League Soccer",
}

FD_API_COUNTRY_MAP = {
    "PL": "England",
    "CL": "World",
    "BL1": "Germany",
    "SA": "Italy",
    "FL1": "France",
    "PPL": "Portugal",
    "EC": "World",
    "WC": "World",
    "ELC": "England",
    "SP1": "Spain",
}

def find_api_league_id(csv_league: str, csv_country: str) -> Optional[int]:
    """Uses the alias map and API map to find a league ID."""
    clean_league_name = LEAGUE_NAME_ALIAS_MAP.get(csv_league, csv_league)
    norm_league = normalize_name(clean_league_name)
    norm_country = normalize_name(csv_country)
    
    # Attempt 1: Exact match with alias/clean name
    key = f"{norm_league}|{norm_country}"
    if key in LEAGUE_NAME_MAP:
        return LEAGUE_NAME_MAP[key]
    
    # Attempt 2: Handle common/specific mismatches
    if norm_country == "england" and norm_league == "premierleague":
        key = "premierleague|england"
    elif norm_country == "spain" and norm_league == "laliga":
        key = "laliga|spain"
    elif norm_league == "ligaportugal":
        key = "primeiraliga|portugal"
    elif norm_country in ["usa", "unitedstates"] and (norm_league == "mls" or norm_league == "majorleaguesoccer"):
        # Handle MLS variants by prioritizing 'Major League Soccer|United States'
        if "majorleaguesoccer|unitedstates" in LEAGUE_NAME_MAP:
             logging.debug(f"Mapped {csv_league}/{csv_country} to Major League Soccer/United States")
             return LEAGUE_NAME_MAP["majorleaguesoccer|unitedstates"]
        key_us = f"majorleaguesoccer|unitedstates" # Fallback key if name is MLS or Major League Soccer
        if key_us in LEAGUE_NAME_MAP:
            logging.debug(f"Mapped {csv_league}/{csv_country} to {clean_league_name}/United States")
            return LEAGUE_NAME_MAP[key_us]
            
    # Final check after specific handlers
    if key in LEAGUE_NAME_MAP:
        return LEAGUE_NAME_MAP[key]
    
    logging.warning(f"No match found for league: '{csv_league}' (Clean: '{clean_league_name}') in country '{csv_country}'")
    return None

def find_closest_season_year(api_league_id: int, date_time: datetime) -> Optional[int]:
    """
    Finds the correct season year for a given match date.
    If no season is found in API data, creates a placeholder league_season.
    """
    season_year = date_time.year
    # Check if the match falls into a season that spans the previous year
    if date_time.month < 7:
        season_year_prev = date_time.year - 1
        season_key_prev = f"{api_league_id}_{season_year_prev}"
        if season_key_prev in LEAGUE_SEASONS_DB:
            return season_year_prev
    
    # Check current year season
    season_key = f"{api_league_id}_{season_year}"
    if season_key in LEAGUE_SEASONS_DB:
        return season_year
    
    # Check surrounding years (just in case the league calendar is unusual)
    season_key_prev = f"{api_league_id}_{season_year - 1}"
    if season_key_prev in LEAGUE_SEASONS_DB:
        return season_year - 1
    season_key_next = f"{api_league_id}_{season_year + 1}"
    if season_key_next in LEAGUE_SEASONS_DB:
        return season_year + 1
        
    # Fallback: Create a new placeholder league_season
    logging.warning(
        f"Could not find existing season for league {api_league_id} near {season_year} "
        f"(tried {season_year}, {season_year-1}, {season_year+1}). "
        f"Creating a new placeholder season for {season_year}."
    )
    start_date = f"{season_year}-01-01"
    end_date = f"{season_year}-12-31"
    get_or_create_league_season(api_league_id, season_year, start_date, end_date, is_current=False)
    return season_year

# ============ STEP 3: OFFLINE - PROCESS CSVs ============
def process_fd_api_csv():
    """Processes 'fd_historical_data_2022-2025.csv'"""
    global FIXTURE_ID_COUNTER
    logging.info(f"--- STEP 2: Processing {FD_API_CSV} ---")
    if not Path(FD_API_CSV).exists():
        logging.warning(f"{FD_API_CSV} not found. Skipping.")
        return
    processed_count = 0
    try:
        # Use utf-8-sig to handle BOM consistently
        with open(FD_API_CSV, mode="r", encoding="utf-8-sig") as f:
            reader = csv.DictReader(f)
            for row in reader:
                try:
                    # Clean row keys (handles BOM and leading/trailing whitespace in headers)
                    keys = list(row.keys())
                    for k in keys:
                        new_k = k.strip('\ufeff').strip()
                        if new_k != k:
                            row[new_k] = row.pop(k)
                    # Remove bad keys (should be rare if cleaning above is robust)
                    bad_keys = [k for k in list(row.keys()) if not k or k.lower() == 'unknown']
                    for k in bad_keys:
                        del row[k]
                    
                    # Check required fields
                    required_fields = ['LeagueCode', 'DateTimeUTC', 'HomeTeam', 'AwayTeam', 'FTHG', 'FTAG']
                    missing_fields = [field for field in required_fields if field not in row or not row[field]]
                    if missing_fields:
                        # Log the problematic row content for better debugging
                        logging.warning(f"Skipping row missing or empty required fields {missing_fields}. Keys: {list(row.keys())}, Data Sample: {str({k: row.get(k) for k in required_fields if k in row})[:50]}...")
                        continue
                    
                    # 1. Find League
                    league_code = row['LeagueCode']
                    if league_code not in FD_API_COUNTRY_MAP:
                        logging.warning(f"Skipping row, unknown LeagueCode: {league_code}")
                        continue
                    csv_league = league_code
                    csv_country = FD_API_COUNTRY_MAP[league_code]
                    api_league_id = find_api_league_id(csv_league, csv_country)
                    
                    if not api_league_id:
                        logging.warning(f"Skipping row, could not map LeagueCode {league_code}")
                        continue
                        
                    # 2. Parse Date
                    date_time = parse_fd_api_date(row['DateTimeUTC'])
                    if not date_time:
                        logging.warning(f"Skipping row for {row['HomeTeam']} vs {row['AwayTeam']}, no valid date.")
                        continue
                        
                    # 3. Find Season Year
                    season_year = find_closest_season_year(api_league_id, date_time)
                    if season_year is None:
                        logging.error(f"FATAL: find_closest_season_year returned None for league {api_league_id}")
                        continue
                        
                    # 4. Get/Create Entities
                    home_team_id = get_or_create_team(row['HomeTeam'])
                    away_team_id = get_or_create_team(row['AwayTeam'])
                    referee = row.get("Referee") or "Unknown Referee"
                    
                    # 5. Compute Scores and Winners
                    ft_home = safe_int(row['FTHG'], 0)
                    ft_away = safe_int(row['FTAG'], 0)
                    
                    ht_home = safe_int(row.get("HTHG"), 0)
                    ht_away = safe_int(row.get("HTAG"), 0)
                    
                    # Use None as default so we know if the column exists but is blank/invalid (returned as None)
                    et_home_val = safe_int(row.get("ETHG"), None)
                    et_away_val = safe_int(row.get("ETAG"), None)
                    pen_home_val = safe_int(row.get("PenH"), None)
                    pen_away_val = safe_int(row.get("PenA"), None)
                    
                    # Calculate total goals (FT + ET)
                    total_home = ft_home + (et_home_val if et_home_val is not None else 0)
                    total_away = ft_away + (et_away_val if et_away_val is not None else 0)

                    # Winner logic
                    if total_home > total_away:
                        home_winner = True
                        away_winner = False
                    elif total_home < total_away:
                        home_winner = False
                        away_winner = True
                    else:
                        if pen_home_val is not None and pen_away_val is not None:
                            if pen_home_val > pen_away_val:
                                home_winner = True
                                away_winner = False
                            elif pen_home_val < pen_away_val:
                                home_winner = False
                                away_winner = True
                            else:
                                home_winner = False
                                away_winner = False 
                        else:
                            home_winner = False
                            away_winner = False 
                            
                    # Status and Elapsed
                    elapsed = 90
                    status_short = "FT"
                    status_long = "Match Finished"
                    
                    if et_home_val is not None or et_away_val is not None:
                        elapsed = 120
                        status_short = "AET"
                    if pen_home_val is not None or pen_away_val is not None:
                        status_short = "PEN"

                    # 6. Create Fixture
                    fixture_key = f"{date_time.astimezone(pytz.UTC).isoformat()}|{home_team_id}|{away_team_id}"
                    if fixture_key not in FIXTURES_DB:
                        FIXTURE_ID_COUNTER -= 1
                        fixture_id = FIXTURE_ID_COUNTER
                        
                        FIXTURES_DB[fixture_key] = {
                            "fixture_id": fixture_id,
                            "referee": referee.strip(),
                            "timezone": "UTC",
                            "date": date_time.astimezone(pytz.UTC).isoformat(),
                            "timestamp": int(date_time.timestamp()),
                            "status_long": status_long,
                            "status_short": status_short,
                            "elapsed": elapsed,
                            "league_id": api_league_id,
                            "season_year": season_year,
                            "venue_id": None,
                            "home_team_id": home_team_id,
                            "away_team_id": away_team_id,
                            "home_winner": home_winner,
                            "away_winner": away_winner,
                            "goals_home": total_home,
                            "goals_away": total_away,
                            "score_ht_home": ht_home,
                            "score_ht_away": ht_away,
                            "score_ft_home": ft_home,
                            "score_ft_away": ft_away,
                            "score_et_home": et_home_val,
                            "score_et_away": et_away_val,
                            "score_pen_home": pen_home_val,
                            "score_pen_away": pen_away_val,
                        }
                        processed_count += 1
                except Exception as row_e:
                    logging.warning(f"Skipping row in {FD_API_CSV} due to error: {type(row_e).__name__}: {row_e}")
                    continue
    except Exception as e:
        logging.error(f"Failed to process {FD_API_CSV}: {type(e).__name__}: {e}")
    logging.info(f"Processed {processed_count} new fixtures from {FD_API_CSV}.")

def process_fd_uk_folder():
    """Processes all CSVs in the 'Data/' folder."""
    global FIXTURE_ID_COUNTER
    logging.info(f"--- STEP 3: Processing {FD_UK_FOLDER} folder ---")
    folder = Path(FD_UK_FOLDER)
    if not folder.exists() or not folder.is_dir():
        logging.warning(f"Folder '{FD_UK_FOLDER}' not found. Skipping.")
        return
    processed_count = 0
    # Regex patterns for file naming conventions (unchanged)
    re_pattern_code = re.compile(
        r"^(?P<code>[A-Z0-9]+)\s(?P<country>[\w\s]+?)\sFootball\s(?:[\d\s-]+\s)?(?P<league>[\w\s\d\.-]+?)\.csv",
        re.IGNORECASE,
    )
    re_pattern_country_football = re.compile(
        r"^(?P<country>[A-Za-z\s]+?)\sFootball\s(?:[\d\s-]+\s)?(?P<league>[\w\s\d\.-]+?)\.csv",
        re.IGNORECASE,
    )
    re_pattern_country_only = re.compile(
        r"^(?P<country>[A-Za-z\s]+?)\s(?:[\d\s-]+\s)?(?P<league>[\w\s\d\.-]+?)\.csv",
        re.IGNORECASE,
    )
    
    for csv_file in folder.glob("*.csv"):
        # --- File Name Parsing (unchanged) ---
        country, league = None, None
        
        match = re_pattern_code.match(csv_file.name)
        if match:
            parts = match.groupdict()
            country = parts.get("country", "").strip()
            league = parts.get("league", "").strip()
        else:
            match = re_pattern_country_football.match(csv_file.name)
            if match:
                parts = match.groupdict()
                country = parts.get("country", "").strip()
                league = parts.get("league", "").strip()
            else:
                match = re_pattern_country_only.match(csv_file.name)
                if match:
                    parts = match.groupdict()
                    country = parts.get("country", "").strip()
                    league = parts.get("league", "").strip()
                else:
                    logging.warning(f"Skipping file, name format not recognized: {csv_file.name}")
                    continue
                    
        if not country or not league:
            logging.warning(f"Skipping file, could not parse country/league: {csv_file.name}")
            continue
            
        league = re.sub(r"^[\d\s-]+\s", "", league).strip()
        api_league_id = find_api_league_id(league, country)
        
        if not api_league_id:
            logging.warning(f"Skipping file, could not map league: '{league}' (Country: '{country}')")
            continue
            
        logging.info(f"Processing {csv_file.name} as: Country='{country}', League='{league}' (API ID: {api_league_id})")
        
        # --- CSV Content Reading (FIX APPLIED) ---
        try:
            with open(csv_file, mode="r", encoding="utf-8-sig", errors="ignore") as f:
                # Read all relevant lines, stripping whitespace and filtering blank lines/lines without commas
                lines = [line for line in f if line.strip() and "," in line]
                if not lines:
                    logging.warning(f"Skipping empty or invalid file: {csv_file.name}")
                    continue
                    
                # Use a StringIO to treat the list of lines as a virtual file for DictReader
                reader = csv.DictReader(io.StringIO(''.join(lines)))

                for row in reader:
                    try:
                        # 1. Clean and alias row keys
                        keys = list(row.keys())
                        for k in keys:
                            # Clean the key name for comparison
                            clean_k = k.strip('\ufeff').strip()
                            
                            # Apply aliasing first (e.g., MLS format)
                            if clean_k == 'Home':
                                row['HomeTeam'] = row.pop(k)
                            elif clean_k == 'Away':
                                row['AwayTeam'] = row.pop(k)
                            elif clean_k == 'HG':
                                row['FTHG'] = row.pop(k)
                            elif clean_k == 'AG':
                                row['FTAG'] = row.pop(k)
                            # If not an alias and the key needs cleaning (e.g., whitespace/BOM)
                            elif clean_k != k and clean_k:
                                row[clean_k] = row.pop(k)
                            # Remove entirely empty header keys
                            elif not clean_k and k in row:
                                del row[k]
                        
                        # Remove any remaining bad keys (e.g., 'unknown' after cleaning)
                        bad_keys = [k for k in list(row.keys()) if not k or k.lower() == 'unknown']
                        for k in bad_keys:
                            if k in row:
                                del row[k]

                        # Check required fields (using standard names)
                        required_fields = ['Date', 'HomeTeam', 'AwayTeam', 'FTHG', 'FTAG']
                        missing_fields = [field for field in required_fields if field not in row or not row[field]]
                        if missing_fields:
                            logging.warning(f"Skipping row missing or empty required fields {missing_fields} in {csv_file.name}. Keys: {list(row.keys())}. Sample: {str({k: row.get(k) for k in required_fields if k in row})[:50]}...")
                            continue
                            
                        # 2. Parse Date
                        date_time = parse_fd_uk_date(row['Date'])
                        if not date_time:
                            logging.warning(f"Skipping row in {csv_file.name}, no valid date for {row['Date']}.")
                            continue
                            
                        # 3. Find Season Year
                        season_year = find_closest_season_year(api_league_id, date_time)
                        if season_year is None:
                            logging.error(f"FATAL: find_closest_season_year returned None for league {api_league_id}")
                            continue
                            
                        # 4. Get/Create Entities
                        home_team_id = get_or_create_team(row['HomeTeam'])
                        away_team_id = get_or_create_team(row['AwayTeam'])
                        referee = row.get("Referee") or "Unknown Referee"
                        
                        # 5. Compute Scores and Winners
                        ft_home = safe_int(row['FTHG'], 0)
                        ft_away = safe_int(row['FTAG'], 0)
                        ht_home = safe_int(row.get("HTHG"), 0)
                        ht_away = safe_int(row.get("HTAG"), 0)
                        
                        # Use None as default for ET/Pen if columns are missing or values are non-existent/invalid
                        et_home_val = safe_int(row.get("ETHG"), None)
                        et_away_val = safe_int(row.get("ETAG"), None)
                        pen_home_val = safe_int(row.get("PSHG") or row.get("PenH"), None)
                        pen_away_val = safe_int(row.get("PSAG") or row.get("PenA"), None)
                        
                        # Final total score calculation (FT + ET)
                        total_home = ft_home + (et_home_val if et_home_val is not None else 0)
                        total_away = ft_away + (et_away_val if et_away_val is not None else 0)
                        
                        # Winner logic
                        if total_home > total_away:
                            home_winner = True
                            away_winner = False
                        elif total_home < total_away:
                            home_winner = False
                            away_winner = True
                        else:
                            if pen_home_val is not None and pen_away_val is not None:
                                if pen_home_val > pen_away_val:
                                    home_winner = True
                                    away_winner = False
                                elif pen_home_val < pen_away_val:
                                    home_winner = False
                                    away_winner = True
                                else:
                                    home_winner = False
                                    away_winner = False
                            else:
                                home_winner = False
                                away_winner = False
                                
                        # Status and Elapsed
                        elapsed = 90
                        status_short = "FT"
                        status_long = "Match Finished"
                        
                        if et_home_val is not None or et_away_val is not None:
                            elapsed = 120
                            status_short = "AET"
                        if pen_home_val is not None or pen_away_val is not None:
                            status_short = "PEN"

                        # 6. Create Fixture
                        fixture_key = f"{date_time.astimezone(pytz.UTC).isoformat()}|{home_team_id}|{away_team_id}"
                        if fixture_key not in FIXTURES_DB:
                            FIXTURE_ID_COUNTER -= 1
                            fixture_id = FIXTURE_ID_COUNTER
                            FIXTURES_DB[fixture_key] = {
                                "fixture_id": fixture_id,
                                "referee": referee.strip(),
                                "timezone": "UTC",
                                "date": date_time.astimezone(pytz.UTC).isoformat(),
                                "timestamp": int(date_time.timestamp()),
                                "status_long": status_long,
                                "status_short": status_short,
                                "elapsed": elapsed,
                                "league_id": api_league_id,
                                "season_year": season_year,
                                "venue_id": None,
                                "home_team_id": home_team_id,
                                "away_team_id": away_team_id,
                                "home_winner": home_winner,
                                "away_winner": away_winner,
                                "goals_home": total_home,
                                "goals_away": total_away,
                                "score_ht_home": ht_home,
                                "score_ht_away": ht_away,
                                "score_ft_home": ft_home,
                                "score_ft_away": ft_away,
                                "score_et_home": et_home_val,
                                "score_et_away": et_away_val,
                                "score_pen_home": pen_home_val,
                                "score_pen_away": pen_away_val,
                            }
                            processed_count += 1
                    except Exception as row_e:
                        logging.warning(f"Skipping row in {csv_file.name} due to error: {type(row_e).__name__}: {row_e}")
                        continue
        except Exception as e:
            logging.error(f"Failed to read {csv_file.name}: {type(e).__name__}: {e}")
    logging.info(f"Processed {processed_count} new fixtures from {FD_UK_FOLDER}.")

# ============ STEP 4: OFFLINE - WRITE OUTPUT CSVS ============
def write_output_csvs():
    """Writes all in-memory DBs to CSV files in the output folder."""
    logging.info(f"--- STEP 4: Writing CSV files to {OUTPUT_FOLDER} ---")
    output_dir = Path(OUTPUT_FOLDER)
    output_dir.mkdir(exist_ok=True)
    
    def write_csv(filename: str, data: list, headers: list):
        if not data:
            logging.info(f"No data for {filename}, skipping.")
            return
        filepath = output_dir / filename
        try:
            with open(filepath, "w", newline="", encoding="utf-8") as f:
                writer = csv.DictWriter(f, fieldnames=headers, extrasaction="ignore")
                writer.writeheader()
                writer.writerows(data)
            logging.info(f"Successfully wrote {len(data)} rows to {filename}.")
        except Exception as e:
            logging.error(f"Failed to write {filename}: {type(e).__name__}: {e}")
            
    # Write Countries
    write_csv("countries.csv", list(COUNTRIES_DB.values()), ["name", "code", "flag_url"])
    # Write Leagues
    write_csv("leagues.csv", list(LEAGUES_DB.values()), ["league_id", "name", "type", "logo_url", "country_name"])
    # Write Seasons
    seasons_data = [{"year": y} for y in sorted(SEASONS_SET)]
    write_csv("seasons.csv", seasons_data, ["year"])
    # Write League_Seasons (omit id)
    write_csv("league_seasons.csv", list(LEAGUE_SEASONS_DB.values()), ["league_id", "season_year", "start_date", "end_date", "is_current"])
    # Write Teams (no matches)
    write_csv("teams.csv", list(TEAMS_DB.values()), ["team_id", "name", "code", "country", "founded", "national", "logo_url", "venue_id"])
    # Write Fixtures (omit created_at)
    fixture_headers = [
        "fixture_id", "referee", "timezone", "date", "timestamp",
        "status_long", "status_short", "elapsed",
        "league_id", "season_year", "venue_id",
        "home_team_id", "away_team_id",
        "home_winner", "away_winner", "goals_home", "goals_away",
        "score_ht_home", "score_ht_away", "score_ft_home", "score_ft_away",
        "score_et_home", "score_et_away", "score_pen_home", "score_pen_away"
    ]
    write_csv("fixtures.csv", list(FIXTURES_DB.values()), fixture_headers)
    
    # Print import order
    print("\nImport the CSVs into Supabase in this order to avoid foreign key errors:")
    print("1. countries.csv")
    print("2. seasons.csv")
    print("3. leagues.csv")
    print("4. league_seasons.csv")
    print("5. teams.csv")
    print("6. fixtures.csv")

# ============ MAIN EXECUTION ============
if __name__ == "__main__":
    logging.info("--- Supabase CSV Converter Started ---")
    # Step 1: Online - Get master league data
    fetch_api_football_leagues()
    # Step 2: Offline - Process fd-api CSV
    process_fd_api_csv()
    # Step 3: Offline - Process fd-uk folder
    process_fd_uk_folder()
    # Step 4: Offline - Write all results
    write_output_csvs()
    logging.info("--- Script Finished Successfully ---")